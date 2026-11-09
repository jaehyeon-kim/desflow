"""The read-back of the Use Avro and Pydantic page: prints the first prediction of
ml-predictions, decoded with the schema the message names. Run it with
`python avro_and_pydantic_check.py` after a shop has run, while `odctl up kafka-lite`
is up."""

# --8<-- [start:check]
import asyncio

from aiokafka import AIOKafkaConsumer

from desflow.connectors.ingress.kafka import ConfluentAvroDeserializer


async def show_first_prediction() -> None:
    """Prints the first message of ml-predictions, decoded with the schema it names."""
    consumer = AIOKafkaConsumer(
        "ml-predictions",
        bootstrap_servers="127.0.0.1:9092",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        message = await asyncio.wait_for(consumer.getone(), timeout=10)
        reader = ConfluentAvroDeserializer(registry_url="http://127.0.0.1:8081")
        print(reader.deserialize("ml-predictions", message.value))
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(show_first_prediction())
# --8<-- [end:check]
