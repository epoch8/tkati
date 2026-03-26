from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import yaml


# ---------------------------------------------------------------------------
# Retention parsing
# ---------------------------------------------------------------------------

_RETENTION_UNITS: list[tuple[str, int]] = [
    ("d",  86_400_000),
    ("h",   3_600_000),
    ("m",      60_000),
    ("s",       1_000),
    ("ms",          1),
]


def _retention_ms(retention: str) -> str:
    """Return Kafka retention.ms value as a string, or '-1' for 'forever'."""
    if retention == "forever":
        return "-1"
    for suffix, mult in _RETENTION_UNITS:
        if retention.endswith(suffix):
            return str(int(retention[: -len(suffix)]) * mult)
    raise ValueError(f"Unrecognised retention format: {retention!r}")


# ---------------------------------------------------------------------------
# Port assignment
# ---------------------------------------------------------------------------

# Each cluster gets a set of ports (offset by cluster index n):
#   external Kafka listener  → 19092 + n
#   Redpanda admin HTTP      →  9644 + n
#   Redpanda Console UI      →  8080 + n
_KAFKA_BASE_PORT = 19092
_ADMIN_BASE_PORT = 9644
_CONSOLE_BASE_PORT = 8080


def _cluster_ports(manifest: dict[str, Any]) -> dict[str, dict[str, int]]:
    """Return {cluster_name: {kafka_port, admin_port}} for every cluster."""
    return {
        name: {
            "kafka_port":   _KAFKA_BASE_PORT + i,
            "admin_port":   _ADMIN_BASE_PORT + i,
            "console_port": _CONSOLE_BASE_PORT + i,
        }
        for i, name in enumerate(sorted(manifest["kafka_clusters"]))
    }


# ---------------------------------------------------------------------------
# docker-compose generation
# ---------------------------------------------------------------------------

def generate_compose(manifest: dict[str, Any]) -> tuple[dict[str, Any], dict[str, dict[str, int]]]:
    """
    Build a docker-compose dict for all clusters in the manifest.

    Returns (compose_dict, ports) where ports maps
    cluster_name → {kafka_port, admin_port}.
    """
    ports = _cluster_ports(manifest)
    services: dict[str, Any] = {}

    for cluster_name, cluster in manifest["kafka_clusters"].items():
        p = ports[cluster_name]
        svc = f"redpanda-{cluster_name}"

        services[svc] = {
            "image": "docker.redpanda.com/redpandadata/redpanda:latest",
            "command": [
                "redpanda", "start",
                f"--kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:{p['kafka_port']}",
                f"--advertise-kafka-addr=internal://{svc}:9092,external://localhost:{p['kafka_port']}",
                f"--rpc-addr={svc}:33145",
                f"--advertise-rpc-addr={svc}:33145",
                "--mode=dev-container",
                "--smp=1",
                "--default-log-level=warn",
            ],
            "ports": [
                f"{p['kafka_port']}:{p['kafka_port']}",
                f"{p['admin_port']}:9644",
            ],
            "healthcheck": {
                "test": ["CMD-SHELL", "rpk cluster health | grep -E 'Healthy:\\s+true'"],
                "interval": "3s",
                "timeout": "5s",
                "retries": 20,
            },
        }

    for cluster_name, p in ports.items():
        svc = f"redpanda-{cluster_name}"
        services[f"console-{cluster_name}"] = {
            "image": "docker.redpanda.com/redpandadata/console:latest",
            "environment": {
                "KAFKA_BROKERS": f"{svc}:9092",
                "REDPANDA_ADMINAPI_ENABLED": "true",
                "REDPANDA_ADMINAPI_URLS": f"http://{svc}:9644",
            },
            "ports": [f"{p['console_port']}:8080"],
            "depends_on": {svc: {"condition": "service_healthy"}},
        }

    services.update(_node_services(manifest))
    return {"services": services}, ports


# ---------------------------------------------------------------------------
# Node service generation
# ---------------------------------------------------------------------------

def _internal_broker(cluster_name: str) -> str:
    return f"redpanda-{cluster_name}:9092"


def _node_env(node: dict[str, Any], manifest: dict[str, Any]) -> dict[str, str]:
    schemas = manifest["schemas"]
    kafka_topics = manifest["kafka_topics"]

    def topic_info(topic_key: str) -> tuple[str, str, list, str]:
        t = kafka_topics[topic_key]
        return t["name"], _internal_broker(t["cluster"]), schemas[t["schema"]]["fields"], t.get("encoding", "arrow-batch")

    env: dict[str, str] = {}

    if node["inputs"]:
        input_list = []
        for inp in node["inputs"]:
            name, broker, fields, encoding = topic_info(inp["topic"])
            entry: dict[str, Any] = {
                "topic": name,
                "brokers": broker,
                "data_schema": fields,
                "encoding": encoding,
                "group": node["name"],
            }
            if inp.get("buffer_size") is not None:
                entry["buffer_size"] = inp["buffer_size"]
            if inp.get("timeout") is not None:
                entry["timeout"] = inp["timeout"]
            input_list.append(entry)
        env["INPUTS"] = json.dumps(input_list)

    if node["outputs"]:
        output_list = []
        for out in node["outputs"]:
            name, broker, fields, encoding = topic_info(out["topic"])
            entry = {
                "topic": name,
                "brokers": broker,
                "data_schema": fields,
                "encoding": encoding,
            }
            if out.get("key") is not None:
                entry["key"] = out["key"]
            output_list.append(entry)
        env["OUTPUTS"] = json.dumps(output_list)

    if node.get("config"):
        env["TKATI_CONFIG"] = json.dumps(node["config"])

    return env


def _node_services(manifest: dict[str, Any]) -> dict[str, Any]:
    kafka_topics = manifest["kafka_topics"]
    services: dict[str, Any] = {}

    for node_name, node in manifest["nodes"].items():
        all_topic_keys = (
            [inp["topic"] for inp in node["inputs"]]
            + [out["topic"] for out in node["outputs"]]
        )
        depends_on = {
            f"redpanda-{kafka_topics[tk]['cluster']}": {"condition": "service_healthy"}
            for tk in all_topic_keys
        }

        svc: dict[str, Any] = {
            "image": node["deploy"]["image"],
            "command": [node["handler"]],
            "environment": _node_env(node, manifest),
            "restart": "on-failure",
        }
        if depends_on:
            svc["depends_on"] = depends_on

        services[node_name] = svc

    return services


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

def write_compose_file(compose: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(compose, default_flow_style=False, sort_keys=False))


def start_compose(compose_path: Path, services: list[str] | None = None) -> None:
    cmd = ["docker", "compose", "-f", str(compose_path), "up", "-d"]
    if services:
        cmd.extend(services)
    subprocess.run(cmd, check=True)


def wait_for_ready(admin_port: int, timeout: int = 60) -> None:
    """Poll the Redpanda admin health endpoint until it returns 200."""
    url = f"http://localhost:{admin_port}/v1/status/ready"
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return
        except Exception as exc:
            last_error = exc
        time.sleep(1)

    raise TimeoutError(
        f"Redpanda on admin port {admin_port} did not become ready within {timeout}s "
        f"(last error: {last_error})"
    )


# ---------------------------------------------------------------------------
# Topic provisioning
# ---------------------------------------------------------------------------

def provision_topics(manifest: dict[str, Any], ports: dict[str, dict[str, int]]) -> list[str]:
    """
    Create all topics from the manifest.  Returns list of created topic names.
    Already-existing topics are silently skipped.
    """
    from kafka.admin import KafkaAdminClient, NewTopic  # type: ignore[import-untyped]
    from kafka.errors import TopicAlreadyExistsError    # type: ignore[import-untyped]

    # Group topics by cluster
    by_cluster: dict[str, list[NewTopic]] = {name: [] for name in manifest["kafka_clusters"]}

    for topic in manifest["kafka_topics"].values():
        cluster_name: str = topic["cluster"]
        retention = topic.get("retention", "7d")
        compacted: bool = topic.get("compacted", False)

        topic_configs = {"retention.ms": _retention_ms(retention)}
        if compacted:
            topic_configs["cleanup.policy"] = "compact"

        by_cluster[cluster_name].append(
            NewTopic(
                name=topic["name"],
                num_partitions=topic.get("partitions", 1),
                replication_factor=1,          # local dev: single-node, RF must be 1
                topic_configs=topic_configs,
            )
        )

    created: list[str] = []

    for cluster_name, new_topics in by_cluster.items():
        if not new_topics:
            continue
        broker = f"localhost:{ports[cluster_name]['kafka_port']}"
        admin = KafkaAdminClient(bootstrap_servers=[broker])
        try:
            admin.create_topics(new_topics, validate_only=False)
            created.extend(t.name for t in new_topics)
        except TopicAlreadyExistsError:
            # Partial creation: some may have been created; treat all as present
            created.extend(t.name for t in new_topics)
        finally:
            admin.close()

    return created
