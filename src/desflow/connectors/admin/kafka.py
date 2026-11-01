import asyncio
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any, DefaultDict, Dict, List, Mapping

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import NotControllerError, TopicAlreadyExistsError, for_code


TOPIC_MODES = ("create", "drop_and_create", "manual")
_POLICY_KEYS = ("mode", "partitions", "replication_factor", "topic_configs")


def topic_policy(spec: Any, where: str = "topic_policy") -> Dict[str, Any]:
    """
    Reads a `topic_policy` into its four settings.

    Args:
        spec: None for the defaults, a mode word such as `manual`, or a mapping of
            `mode`, `partitions`, `replication_factor` and `topic_configs`.
        where: The key the spec was read from, named in errors.

    Returns:
        Dict[str, Any]: `mode` (default `create`), `partitions` (1),
            `replication_factor` (1) and `topic_configs` (empty, with text values).

    Raises:
        ValueError: If the mode is not one of `create`, `drop_and_create` and
            `manual`, a key is unknown, or a number is not a positive whole number.
    """
    if spec is None:
        spec = {}
    elif isinstance(spec, str):
        spec = {"mode": spec}
    if not isinstance(spec, Mapping):
        raise ValueError(f"{where} must be a mode word or a mapping, got {spec!r}")
    unknown = sorted(set(spec) - set(_POLICY_KEYS))
    if unknown:
        raise ValueError(
            f"{where} has unknown key(s) {unknown}; the keys are {', '.join(_POLICY_KEYS)}"
        )
    mode = spec.get("mode", "create")
    if mode not in TOPIC_MODES:
        raise ValueError(
            f"{where} mode '{mode}' is not one of {', '.join(TOPIC_MODES)}"
        )
    numbers = {}
    for key in ("partitions", "replication_factor"):
        value = spec.get(key, 1)
        if type(value) is not int or value < 1:
            raise ValueError(f"{where} {key} must be a whole number of 1 or more")
        numbers[key] = value
    configs = spec.get("topic_configs") or {}
    if not isinstance(configs, Mapping):
        raise ValueError(f"{where} topic_configs must be a mapping of setting to value")
    return {
        "mode": mode,
        **numbers,
        "topic_configs": {str(k): str(v) for k, v in configs.items()},
    }


def build_ssl_context(spec: Any) -> Any:
    """
    Returns the SSL context of an `ssl` setting.

    Args:
        spec: A mapping of `cafile`, `certfile`, `keyfile` and `password`, the
            arguments of `aiokafka.helpers.create_ssl_context`, or an `SSLContext`,
            which is returned as it is.

    Returns:
        Any: The `ssl.SSLContext`.

    Raises:
        ValueError: If a key is not one of those four.
    """
    if not isinstance(spec, Mapping):
        return spec
    from aiokafka.helpers import create_ssl_context

    unknown = sorted(set(spec) - {"cafile", "certfile", "keyfile", "password"})
    if unknown:
        raise ValueError(
            f"ssl has unknown key(s) {unknown}; the keys are cafile, certfile, "
            "keyfile and password"
        )
    return create_ssl_context(**spec)


def build_token_provider(spec: Any) -> Any:
    """
    Returns the token provider of a `sasl_oauth_token_provider` setting.

    Args:
        spec: A mapping `{type: aws_msk_iam, region: <region>}`, or a token
            provider object, which is returned as it is.

    Returns:
        Any: An `AbstractTokenProvider` that signs a token for Amazon MSK IAM with
            the AWS credentials of the environment.

    Raises:
        ValueError: If the type is not `aws_msk_iam`, or `region` is missing.
        ImportError: If `aws-msk-iam-sasl-signer-python` is not installed.
    """
    if not isinstance(spec, Mapping):
        return spec
    unknown = sorted(set(spec) - {"type", "region"})
    if unknown or spec.get("type") != "aws_msk_iam" or not spec.get("region"):
        raise ValueError(
            "sasl_oauth_token_provider takes {type: aws_msk_iam, region: <region>}, "
            f"got {dict(spec)!r}"
        )
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    except ImportError:
        raise ImportError(
            "sasl_oauth_token_provider type aws_msk_iam needs the package "
            "aws-msk-iam-sasl-signer-python. Install it with "
            "pip install aws-msk-iam-sasl-signer-python"
        ) from None
    from aiokafka.abc import AbstractTokenProvider

    region = str(spec["region"])

    class MSKTokenProvider(AbstractTokenProvider):
        async def token(self) -> str:
            # Uses the AWS credentials of the environment, or the EC2 or EKS role
            token, _ = MSKAuthTokenProvider.generate_auth_token(region)
            return token

    return MSKTokenProvider()


def apply_topic_policies(
    admin: "KafkaAdminConnector", policies: Dict[str, Dict[str, Any]]
) -> None:
    """
    Creates, recreates or leaves alone each topic, as its policy says.

    `drop_and_create` topics are deleted first, once the broker no longer lists them.
    Then every topic that is not `manual` is created in one request, and one that
    exists is left as it is. A topic with the default settings is asked for by name
    alone.

    Args:
        admin: The connector that talks to the broker.
        policies: Topic name to a policy as `topic_policy` returns it.

    Raises:
        KafkaError: If the broker reports an error for a topic.
        TimeoutError: If a deleted topic is still listed after 30 seconds.
    """
    dropped = [n for n, p in policies.items() if p["mode"] == "drop_and_create"]
    if dropped:
        admin.drop_topics(dropped)
    configs = []
    for name, policy in policies.items():
        if policy["mode"] == "manual":
            continue
        config: Dict[str, Any] = {"name": name}
        if policy["partitions"] != 1:
            config["partitions"] = policy["partitions"]
        if policy["replication_factor"] != 1:
            config["replication"] = policy["replication_factor"]
        if policy["topic_configs"]:
            config["topic_configs"] = policy["topic_configs"]
        configs.append(config)
    if configs:
        admin.create_topics(topics_config=configs)


class KafkaAdminConnector:
    """
    Unified Kafka Admin and Monitoring Connector.

    This connector acts as a management layer for the simulation's Kafka
    infrastructure. It creates topics, sends config and collects telemetry and
    events, all with `aiokafka`; `create_topics` is a synchronous method.

    It maintains an internal state of simulation vitals and task lifecycles
    by consuming from the simulation's egress topics.

    Attributes:
        bootstrap_servers (str): Kafka broker addresses.
        max_tasks (int): The maximum number of task records to keep per service
            in memory to prevent unbounded growth.
        kwargs (dict): Additional arguments passed to Kafka clients.
    """

    def __init__(self, bootstrap_servers: str, max_tasks: int = 100, **kwargs: Any):
        """
        Initialize the connector with broker settings and state limits.

        Args:
            bootstrap_servers: Kafka broker addresses.
            max_tasks: The maximum number of task records to keep per service
                in memory (rolling window).
            **kwargs: Additional arguments passed to Kafka clients (e.g., security settings).
        """
        self.bootstrap_servers = bootstrap_servers
        self.max_tasks = max_tasks
        self.kwargs = kwargs

        # Event State: sim_id -> service -> task_id -> {status: timestamp}
        self._state: DefaultDict[str, DefaultDict[str, Dict[str, Any]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        # Telemetry State: path_id -> latest_value
        self._vitals: Dict[str, Any] = {}

    def create_topics(self, topics_config: List[Dict[str, Any]]):
        """
        Creates Kafka topics required for the simulation.

        This is a synchronous call to ensure that all necessary infrastructure
        (config, telemetry, and event topics) is ready before the simulation
        environment starts. It runs the asynchronous admin client to completion. When
        the calling thread already runs an event loop, it does so in a worker
        thread. A topic that already exists is ignored. A topic error that says the
        broker is not the controller is retried once with a new client. Any other
        error that the broker reports for a topic is raised.

        Args:
            topics_config: A list of topic configurations.
                Each dict should contain 'name', and optionally 'partitions',
                'replication' and 'topic_configs', a mapping of topic setting to
                value such as `{"retention.ms": "60000"}`.

        Raises:
            TypeError: If a setting passed to the connector is not one of
                `AIOKafkaAdminClient`'s, such as the kafka-python setting
                `ssl_cafile`. Pass `ssl_context` instead.
            KafkaError: If the broker reports an error for a topic, such as an
                invalid replication factor. The error is the `aiokafka.errors`
                class for the error code. A topic that exists is not an error.
        """
        new_topics = [
            NewTopic(
                name=cfg["name"],
                num_partitions=cfg.get("partitions", 1),
                replication_factor=cfg.get("replication", 1),
                topic_configs=cfg.get("topic_configs") or None,
            )
            for cfg in topics_config
        ]
        self._run_to_completion(self._create_topics(new_topics))

    def drop_topics(self, names: List[str]) -> None:
        """
        Deletes the topics that exist, and waits until the broker lists none of them.

        Args:
            names: The topic names. A name that does not exist is ignored.

        Raises:
            TimeoutError: If a deleted topic is still listed after 30 seconds.
            KafkaError: If the broker refuses the request.
        """
        self._run_to_completion(self._drop_topics(names))

    def _run_to_completion(self, coroutine: Any) -> None:
        """Runs a coroutine to its end, in a worker thread when a loop is running."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(coroutine)
            return
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(asyncio.run, coroutine).result()

    async def _drop_topics(self, names: List[str]) -> None:
        """Deletes the topics that exist, and waits until the broker lists none."""
        admin_client = AIOKafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id="sim_admin",
            **self.kwargs,
        )
        try:
            await admin_client.start()
            existing = [
                name for name in names if name in await admin_client.list_topics()
            ]
            if not existing:
                return
            await admin_client.delete_topics(existing)
            for _ in range(60):
                if not set(existing) & set(await admin_client.list_topics()):
                    return
                await asyncio.sleep(0.5)
            raise TimeoutError(
                f"The topics {existing} are still listed after 30 seconds"
            )
        finally:
            await admin_client.close()

    async def _create_topics(self, new_topics: List[NewTopic]):
        """Creates the topics, once more with a new client after a NOT_CONTROLLER error."""
        topic_errors = await self._request_topics(new_topics)
        if any(error[1] == NotControllerError.errno for error in topic_errors):
            # kafka-python refreshed the controller and sent the request again. A new
            # client fetches fresh metadata, which names the current controller.
            topic_errors = await self._request_topics(new_topics)
        for topic_error in topic_errors:
            topic, error_code = topic_error[0], topic_error[1]
            if error_code not in (0, TopicAlreadyExistsError.errno):
                message = topic_error[2] if len(topic_error) > 2 else None
                raise for_code(error_code)(f"Topic {topic}: {message or 'no message'}")

    async def _request_topics(self, new_topics: List[NewTopic]) -> List[Any]:
        """Sends one CreateTopics request with an admin client that lives for this call."""
        admin_client = AIOKafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id="sim_admin",
            **self.kwargs,
        )
        try:
            await admin_client.start()
            response = await admin_client.create_topics(
                new_topics=new_topics, validate_only=False
            )
            # aiokafka returns the broker's answer and raises no error for a topic.
            # In every CreateTopics response version an entry of `topic_errors`
            # starts with the topic and its error code, and from version 1 it ends
            # with the broker's message.
            return list(response.topic_errors)
        finally:
            await admin_client.close()

    async def send_config(self, topic: str, path: str, value: Any):
        """
        Sends a surgical parameter update to a simulation config topic.

        This method acts as an external controller, allowing users to
        dynamically update registry paths (e.g., arrival rates or resource
        capacities) while the simulation is running. The message is the JSON
        object `{"path": path, "value": value}`, the format `KafkaIngress` reads.

        Args:
            topic: The Kafka topic the simulation is listening to for config.
            path: The registry dot-notation path (e.g., 'Shop.resources.packers.max_cap').
            value: The new value to be applied to the path.
        """
        producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers, **self.kwargs
        )
        try:
            await producer.start()  # inside the try, so a failed start closes it
            payload = json.dumps({"path": path, "value": value}).encode("utf-8")
            await producer.send_and_wait(topic, payload)
        finally:
            await producer.stop()

    async def collect_data(self, topics: List[str], auto_offset_reset: str = "latest"):
        """
        Async loop to consume from telemetry and event topics.

        This loop continuously listens to the simulation's egress and updates
        the connector's internal `_vitals` and `_state` attributes.

        Args:
            topics: List of topics to consume from (telemetry and events).
            auto_offset_reset: Where to start consuming if no offset is committed.
        """
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset=auto_offset_reset,
            **self.kwargs,
        )
        try:
            await consumer.start()  # inside the try, so a failed start closes it
            async for msg in consumer:
                data = json.loads(msg.value.decode("utf-8"))
                self._process_message(data)
        finally:
            await consumer.stop()

    def _process_message(self, data: Dict[str, Any]):
        """
        Routes incoming messages based on their JSON structure.

        Distinguishes between 'telemetry' (vitals like utilization) and
        'events' (task lifecycle steps like 'started' or 'finished').

        Args:
            data: The decoded JSON payload from Kafka.
        """

        # 1. Telemetry: 'path_id' is at the root level
        if "path_id" in data and not isinstance(data.get("value"), dict):
            self._vitals[data["path_id"]] = data["value"]

        # 2. Events: 'key' is at the root, 'value' is a dictionary
        elif "key" in data and isinstance(data.get("value"), dict):
            task_id = data["key"]
            payload = data["value"]
            status = payload.get("status")

            # Using 'timestamp' based on your actual JSON payload
            ts = data.get("timestamp")

            path_id = payload.get("path_id", "unknown.unknown.unknown")
            parts = path_id.split(".")
            sim_id = parts[0]
            service = parts[2] if len(parts) > 2 else "default"

            service_data = self._state[sim_id][service]

            # Prune oldest if at max capacity
            if task_id not in service_data:
                if len(service_data) >= self.max_tasks:
                    oldest_key = next(iter(service_data))
                    service_data.pop(oldest_key)

            # Update task status timestamp
            if task_id not in service_data:
                service_data[task_id] = {}

            service_data[task_id][status] = ts

    def get_vitals(self) -> Dict[str, Any]:
        """
        Returns the latest system telemetry metrics.

        Returns:
            A dictionary of path_id to latest value (e.g., utilization, queue length).
        """
        return self._vitals

    def get_state(self) -> Dict[str, Any]:
        """
        Returns the aggregated event state for task lifecycles.

        Returns:
            A nested dictionary: sim_id -> service -> task_id -> {status: timestamp}.
        """
        return self._state
