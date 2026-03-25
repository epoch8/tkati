local tk = import 'tkati.libsonnet';

{
  clusters: {
    prod: tk.kafka_cluster('prod',
      brokers=['redpanda-prod:9092'],
      defaults={ replication_factor: 3, retention: '7d' },
    ),
  },
  image: { repo: 'my-registry/pipelines', tag: '1.0' },
  node_defaults: {
    resources: {
      requests: { cpu: '500m', memory: '512Mi' },
      limits:   { cpu: '1',    memory: '1Gi' },
    },
  },
}
