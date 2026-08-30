# 0.4.0a5

* Initial implementation of `tkati-dashboard`: reads a serialized tkati dataflow directory — a
  directory of JSON fragments, no manifest required, every `*.json` file directly inside it is
  merged into the graph — and serves a local web page rendering it top-to-bottom as a graph with
  React Flow, colored by source/sink vs. processing node. A dataflow's name is its directory's
  own name.
* The graph shows `broker`/`topic` on `kafka-topic` nodes and the consumer `group_id` (plus live
  lag, see below) on edges that carry one
* Clicking a node opens a side panel with its full connection/config/schema. For a `kafka-topic`
  node, the panel also fetches a live snapshot of the topic's most recent messages via
  `GET /api/nodes/{id}/snapshot` and `tkati_dashboard.snapshot` (a throwaway consumer group,
  never commits offsets)
* Stream edges with a `consumer.group_id` show their live consumer lag, via
  `GET /api/nodes/{id}/consumer-lag` and `tkati_dashboard.lag` (reads committed offsets with
  `Consumer.committed()`, never subscribes/polls as the group). Both live-broker lookups show an
  inline error instead of breaking the page when the broker/topic is unreachable
* Added `examples/simple-pipeline` (two topics, one dedup node) with a `seed_kafka.py` script
  that populates it with sample events, including intentional duplicates, and a bigger
  `examples/analytics-pipeline` exercising fragment merging across four files
* The node panel's `kafka-topic` section now also shows topic stats: per-partition
  leader/replicas/in-sync-replicas (flagging under-replication) and topic config
  (`retention.ms`, `cleanup.policy`, etc., each flagged when it differs from the broker
  default), via `GET /api/nodes/{id}/topic-stats` and `tkati_dashboard.topic_stats`
* `schema` is optional for every node type (previously required for `kafka-topic`/
  `clickhouse-table`), since it isn't always on hand for a real-world node
* A fragment can now also declare a single node via a top-level `"node"` object carrying its
  own `"id"`, instead of only `"nodes": {<id>: {...}}` — fixes edges to such a node being
  reported as referencing an unknown node
* `load_dataflow` now also reads `*.yaml`/`*.yml` fragments, freely mixable with `*.json` ones
  in the same directory — both decode to the same nodes/edges structure and merge identically
* The node panel is now an always-visible, resizable inspector (drag its left edge; width
  persisted to `localStorage`) instead of a popup that only exists while a node is selected
* "Latest events" now shows each message as its own pretty-printed JSON block instead of a
  table, since a real message commonly has too many fields for table columns to work in a
  narrow panel
* Every panel section (Connection, Config, Schema, Topic stats, Latest events) is now
  collapsible, without unmounting/re-fetching a live-fetching section when reopened
