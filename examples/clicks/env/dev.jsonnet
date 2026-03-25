local tk = import 'tkati.libsonnet';

{
  clusters: {
    prod: tk.kafka_cluster('prod',
      brokers=['localhost:9092'],
      defaults={ replication_factor: 1, retention: '1d' },
    ),
  },
  image: { repo: 'my-registry/pipelines', tag: 'latest' },
  node_defaults: {
    resources: {
      requests: { cpu: '100m', memory: '256Mi' },
      limits:   { cpu: '500m', memory: '512Mi' },
    },
  },
}
