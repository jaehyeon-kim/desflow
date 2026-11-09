"""Settings shared by the simulation, the Flink jobs, the dashboard and the tests.

The host addresses use 127.0.0.1 rather than localhost, because with IPv6 enabled in
Docker some services reset connections to the IPv6 address of localhost.
"""

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
SCHEMA_REGISTRY = "http://127.0.0.1:8081"
FLINK_REST = "http://127.0.0.1:8082"
DATABASE = "postgresql://user:password@127.0.0.1:5432/odctl"

SIM_ID = "game"  # the simulation's name, and the first part of every registry path
TOPIC = "game-scores"
CONTROL_TOPIC = "game-control"  # parameter changes the running simulation reads
SUBJECT = f"{TOPIC}-value"  # the schema registry subject the Avro serializer uses
SCHEMA = "game"  # the PostgreSQL schema that holds the leaderboard tables

FLINK_CONTAINER = "flink-jobmanager"
# The four Flink jobs, by the `pipeline.name` each job file sets.
JOB_NAMES = ("game-top-teams", "game-top-players", "game-hot-streaks", "game-team-mvps")

APP_PORT = 8091
