export interface SchemaField {
  name: string
  type: string
}

export interface Schema {
  name: string
  fields: SchemaField[]
}

export interface KafkaCluster {
  name: string
  brokers: string[]
}

export interface KafkaTopic {
  name: string
  cluster: string
  schema: string
  partitions: number
  replication_factor: number
  retention: string
  compacted: boolean
}

export interface TopicRef {
  topic: string
  buffer_size: number | null
  timeout: string | null
}

export interface OutputRef {
  topic: string
  key: string | null
}

export interface DeployResources {
  requests?: { cpu: string; memory: string }
  limits?: { cpu: string; memory: string }
}

export interface Deploy {
  image: string
  replicas: number
  resources?: DeployResources
}

export interface PipelineNode {
  name: string
  inputs: TopicRef[]
  outputs: OutputRef[]
  handler: string
  config: Record<string, unknown>
  deploy: Deploy
}

export interface Manifest {
  schemas: Record<string, Schema>
  kafka_clusters: Record<string, KafkaCluster>
  kafka_topics: Record<string, KafkaTopic>
  nodes: Record<string, PipelineNode>
}
