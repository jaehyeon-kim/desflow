"""Helpers for the tests of the How-To pages that write to Kafka, files and tables.

The pages' scripts name real services. A test runs each script with the service replaced:
a Kafka producer that keeps its messages, or a capture in place of a sink. No test here
contacts a broker, a bucket or a catalog.
"""

import asyncio
import importlib.util
import json
import queue
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

from desflow.connectors.egress.base import BaseEgress

ROOT = Path(__file__).resolve().parents[2]
SNIPPETS = ROOT / "docs" / "snippets"
GUIDES = SNIPPETS / "guides"
YAML = SNIPPETS / "yaml"


class Capture(BaseEgress):
    """Keeps every record it receives, in place of a sink."""

    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def load_from(folder, name):
    """Loads `<folder>/<name>.py` as a module, with the folder on the path."""
    sys.path.insert(0, str(folder))
    try:
        spec = importlib.util.spec_from_file_location(name, Path(folder) / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(folder))


def copy_into(tmp_path, *files):
    """Copies the files of a page into one folder, as a reader does."""
    import shutil

    for file in files:
        shutil.copy(file, tmp_path)


def _decoded(value):
    """The JSON a message holds, the bytes of any other message, or None."""
    if value is None:
        return None
    try:
        return json.loads(value)
    except ValueError:
        return value


class Messages(list):
    """The messages a producer was given, and the settings each producer was built with."""

    def __init__(self):
        super().__init__()
        self.settings = []


@contextmanager
def kafka_messages():
    """Replaces the Kafka producer and the admin client, and yields what was sent. Its `settings` hold the arguments each producer was built with.

    The list holds one `(topic, key, value)` for each message a producer was given. The
    value is decoded from JSON, kept as bytes when it is not JSON (an Avro message), or
    None for a tombstone. The admin client's mock is yielded second, so a test can read
    the topics a sink asked it to create.
    """
    sent = Messages()

    def producer(*args, **kwargs):
        sent.settings.append(kwargs)
        fake = AsyncMock()

        async def send(topic, value=None, key=None, **kw):
            sent.append((topic, None if key is None else key.decode(), _decoded(value)))

        fake.send.side_effect = send
        return fake

    with (
        patch("desflow.connectors.egress.kafka.AIOKafkaProducer", side_effect=producer),
        patch("desflow.connectors.egress.kafka.KafkaAdminConnector") as admin,
    ):
        yield sent, admin


def events(messages):
    """(topic, key, task, instance, value) of every JSON event message, without the clock."""
    return [
        (topic, value["key"], value["task"], value["instance"], value["value"])
        for topic, _, value in messages
        if isinstance(value, dict) and "task" in value
    ]


def created_topics(admin):
    """The names of the topics the sinks asked the admin client to create."""
    names = set()
    for call in admin.return_value.create_topics.call_args_list:
        for topic in call.kwargs.get(
            "topics_config", call.args[0] if call.args else []
        ):
            names.add(topic["name"])
    return names


class FakeConsumer:
    """Stands in for AIOKafkaConsumer: serves the messages given, as a broker would."""

    messages: list = []
    settings: dict = {}

    def __init__(self, *topics, **settings):
        type(self).settings = {"topics": topics, **settings}
        self._messages = iter(type(self).messages)

    async def start(self):
        pass

    async def stop(self):
        pass

    async def getone(self):
        from types import SimpleNamespace

        return SimpleNamespace(value=next(self._messages))


class Registry:
    """A schema registry that keeps every schema registered and serves it back by id."""

    def __init__(self):
        self.registered = []

    def __call__(self, request):
        import httpx

        if request.method == "POST":
            self.registered.append(json.loads(request.content))
            return httpx.Response(200, json={"id": len(self.registered)})
        schema_id = int(request.url.path.rsplit("/", 1)[1])
        return httpx.Response(200, json=self.registered[schema_id - 1])
