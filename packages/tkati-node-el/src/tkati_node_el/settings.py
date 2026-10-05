from pydantic import BaseModel
from tkati_core.settings import NodeSettings, OutputSettings


class TransformSettings(BaseModel):
    # Python source defining `transform(batch: pa.Table) -> pa.Table`. Run
    # once at startup as a module; see `tkati_node_el.transform`.
    code: str


class AppSettings(NodeSettings):
    # Required here, unlike in NodeSettings: this node always writes to an
    # output producer. Pydantic intentionally narrows the config field;
    # ty rejects narrowing mutable attributes under normal inheritance rules.
    output: OutputSettings  # ty: ignore[invalid-mutable-override]
    transform: TransformSettings | None = None
