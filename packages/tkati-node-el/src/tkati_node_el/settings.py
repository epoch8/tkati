from tkati_core.settings import NodeSettings, OutputSettings


class AppSettings(NodeSettings):
    # Required here, unlike in NodeSettings: this node always writes to an
    # output producer.
    output: OutputSettings
