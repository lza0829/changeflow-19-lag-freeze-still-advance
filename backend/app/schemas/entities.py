from pydantic import BaseModel


class ApplyRequest(BaseModel):
    stream_id: int


class StreamPatch(BaseModel):
    enabled: bool | None = None


class SettingPut(BaseModel):
    value: str


class LagSlaConfigPut(BaseModel):
    threshold_seconds: int
    policy: str
