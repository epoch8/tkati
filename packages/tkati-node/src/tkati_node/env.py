"""Node configuration: two-layer parsing.

Layer 1 — pydantic settings: reads env vars, validates all fields.
Layer 2 — dataclasses: runtime config with computed Arrow schemas etc.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pyarrow as pa
from pydantic import BaseModel, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from tkati_node.schema import SchemaDef


def _parse_timeout_ms(s: str) -> int:
    """Convert '2s' or '500ms' to milliseconds."""
    s = s.strip()
    if s.endswith("ms"):
        return int(s[:-2])
    if s.endswith("s"):
        return int(float(s[:-1]) * 1000)
    raise ValueError(f"Unrecognised timeout format: {s!r}")


# ===========================================================================
# Layer 1: pydantic — reads env vars, validates everything
# ===========================================================================

class InputSettings(BaseModel):
    topic: str
    brokers: str
    data_schema: SchemaDef
    encoding: Literal["json-per-message", "arrow-batch"]
    group: str
    buffer_size: int | None = None
    timeout: str | None = None

    @field_validator("timeout")
    @classmethod
    def _valid_timeout(cls, v: str | None) -> str | None:
        if v is not None:
            _parse_timeout_ms(v)
        return v


class OutputSettings(BaseModel):
    topic: str
    brokers: str
    data_schema: SchemaDef
    encoding: Literal["json-per-message", "arrow-batch"]
    key: str | None = None


class NodeSettings(BaseModel):
    inputs: list[InputSettings]
    outputs: list[OutputSettings]
    handler: str
    config: dict = {}
    tick_interval_ms: int | None = None


# ---------------------------------------------------------------------------
# Single settings class — superset of all env vars, parsed at once
# ---------------------------------------------------------------------------

class _Env(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__", extra="ignore")

    # Single input/output — nested via INPUT__TOPIC, INPUT__BROKERS, …
    input: InputSettings | None = None
    output: OutputSettings | None = None

    # Multiple inputs/outputs — INPUTS / OUTPUTS as JSON arrays
    inputs: list[InputSettings] | None = None
    outputs: list[OutputSettings] | None = None

    # Handler config — TKATI_CONFIG
    tkati_config: dict = {}

    tick_interval_ms: int | None = None


def load_settings(handler: str) -> NodeSettings:
    """Read and validate all configuration from environment variables."""
    env = _Env()

    if env.input is not None:
        inputs = [env.input]
        outputs = [env.output] if env.output is not None else []
    elif env.inputs is not None:
        inputs = env.inputs
        outputs = env.outputs or []
    else:
        inputs = []
        outputs = env.outputs or []

    return NodeSettings(
        inputs=inputs,
        outputs=outputs,
        handler=handler,
        config=env.tkati_config,
    )


# ===========================================================================
# Layer 2: dataclasses — runtime config with computed values
# ===========================================================================

@dataclass
class InputConfig:
    topic: str
    brokers: str
    data_schema: pa.Schema
    encoding: str
    group: str
    buffer_size: int | None
    timeout_ms: int | None


@dataclass
class OutputConfig:
    topic: str
    brokers: str
    data_schema: pa.Schema
    encoding: str
    key: str | None


@dataclass
class NodeConfig:
    inputs: list[InputConfig]
    outputs: list[OutputConfig]
    handler: str
    config: dict
    tick_interval_ms: int | None = None


def build_runtime_config(settings: NodeSettings) -> NodeConfig:
    """Parse Arrow schemas and timeout durations into runtime dataclasses."""
    return NodeConfig(
        inputs=[
            InputConfig(
                topic=s.topic,
                brokers=s.brokers,
                data_schema=s.data_schema.to_arrow(),
                encoding=s.encoding,
                group=s.group,
                buffer_size=s.buffer_size,
                timeout_ms=_parse_timeout_ms(s.timeout) if s.timeout else None,
            )
            for s in settings.inputs
        ],
        outputs=[
            OutputConfig(
                topic=s.topic,
                brokers=s.brokers,
                data_schema=s.data_schema.to_arrow(),
                encoding=s.encoding,
                key=s.key,
            )
            for s in settings.outputs
        ],
        handler=settings.handler,
        config=settings.config,
        tick_interval_ms=settings.tick_interval_ms,
    )


def load_config(handler: str) -> NodeConfig:
    """Load, validate, and build runtime config from environment variables."""
    return build_runtime_config(load_settings(handler))
