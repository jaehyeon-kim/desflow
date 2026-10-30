import asyncio
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any, DefaultDict, Dict, List

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import NotControllerError, TopicAlreadyExistsError, for_code


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
                Each dict should contain 'name', and optionally 'partitions'
                and 'replication'.

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
            )
            for cfg in topics_config
        ]
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self._create_topics(new_topics))
            return
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(asyncio.run, self._create_topics(new_topics)).result()

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
