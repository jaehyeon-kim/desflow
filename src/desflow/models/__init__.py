"""The base model of the payloads that egress sinks write.

The models a model file names, such as `SimParameter` and `EventPayload`, are exported from
`desflow`.
"""

from desflow.models.schemas import BaseStreamPayload

__all__ = ["BaseStreamPayload"]
