from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Order(_message.Message):
    __slots__ = ("order_id", "amount")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    order_id: int
    amount: float
    def __init__(self, order_id: _Optional[int] = ..., amount: _Optional[float] = ...) -> None: ...

class ShopRecord(_message.Message):
    __slots__ = ("key", "task", "instance", "sim_ts", "timestamp", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    SIM_TS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    task: str
    instance: int
    sim_ts: float
    timestamp: str
    value: Order
    def __init__(self, key: _Optional[str] = ..., task: _Optional[str] = ..., instance: _Optional[int] = ..., sim_ts: _Optional[float] = ..., timestamp: _Optional[str] = ..., value: _Optional[_Union[Order, _Mapping]] = ...) -> None: ...
