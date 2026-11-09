"""The score event, with bounds on every field, and the Avro schema it is sent with."""

import json

from pydantic import BaseModel, Field


class ScoreEvent(BaseModel):
    """
    One score a player earned in a round.

    Attributes:
        user_id (str): The player, `USR-` and a number for a person, `RBT-` for a robot.
        team_id (str): The team's 10-digit id.
        team_name (str): The team, a colour and an animal such as `Amber-Koala`.
        score (int): The points earned, from 0 to 20.
        event_time_millis (int): When the score was earned, in milliseconds since the
            Unix epoch. A late score carries an earlier time than when it is sent.
        event_type (str): `normal`, or `late` for a score sent minutes after it was earned.
    """

    user_id: str = Field(pattern=r"^(USR|RBT)-\d{6,}$")
    team_id: str = Field(pattern=r"^\d{10}$")
    team_name: str = Field(pattern=r"^[A-Z][a-z]+-[A-Z][a-z]+$")
    score: int = Field(ge=0, le=20)
    event_time_millis: int = Field(gt=0)
    event_type: str = Field(pattern=r"^(normal|late)$")


# Flink reads the topic with this schema, so its fields must match the source table in
# jobs/00-ddl.sql.
AVRO_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "ScoreEvent",
        "namespace": "game",
        "fields": [
            {"name": "user_id", "type": "string"},
            {"name": "team_id", "type": "string"},
            {"name": "team_name", "type": "string"},
            {"name": "score", "type": "int"},
            {"name": "event_time_millis", "type": "long"},
            {"name": "event_type", "type": "string"},
        ],
    }
)
