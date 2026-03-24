// Takes a compiled tkati manifest and returns a Terraform JSON configuration.
//
// Usage:
//   local toTerraform = import 'tkati-terraform.libsonnet';
//   toTerraform(manifest, k8s_namespace='pipelines')

function(manifest, k8s_namespace='pipelines')
  // Replace underscores with hyphens for Kubernetes resource names.
  local toKebab(s) = std.join('-', std.split(s, '_'));

  // Replace '/' with '__' for Terraform resource identifiers (cluster/topic → cluster__topic).
  local tfTopicKey(key) = std.join('__', std.split(key, '/'));

  // Convert a retention duration string to milliseconds as a string.
  // Supported suffixes: d, h, m, s. Special value: 'forever' → '-1'.
  local retentionMs(s) =
    if s == 'forever' then '-1'
    else
      local n = std.parseInt(s[0:std.length(s) - 1]);
      local unit = s[std.length(s) - 1:std.length(s)];
      std.toString(
        if unit == 'd' then n * 86400000
        else if unit == 'h' then n * 3600000
        else if unit == 'm' then n * 60000
        else if unit == 's' then n * 1000
        else error 'Unknown retention unit in "' + s + '"'
      );

  // --- kafka_topic resources ---

  local kafkaTopicResources = {
    [tfTopicKey(key)]: {
      provider:           'kafka.' + manifest.kafka_topics[key].cluster,
      name:               manifest.kafka_topics[key].name,
      partitions:         manifest.kafka_topics[key].partitions,
      replication_factor: manifest.kafka_topics[key].replication_factor,
      config: {
        'retention.ms':   retentionMs(manifest.kafka_topics[key].retention),
        'cleanup.policy': if manifest.kafka_topics[key].compacted then 'compact' else 'delete',
      },
    }
    for key in std.objectFields(manifest.kafka_topics)
  };

  // --- kubernetes_deployment resources ---

  local k8sDeploymentResources = {
    [name]:
      local node      = manifest.nodes[name];
      local kebabName = toKebab(name);
      local baseEnv = [
        { name: 'TKATI_INPUTS',  value: std.join(',', [i.topic for i in node.inputs]) },
        { name: 'TKATI_OUTPUTS', value: std.join(',', [o.topic for o in node.outputs]) },
        { name: 'TKATI_CONFIG',  value: std.manifestJsonMinified(node.config) },
      ];
      local extraEnv = if std.objectHas(node.deploy, 'env') then node.deploy.env else [];
      local container = {
        name:    'node',
        image:   node.deploy.image,
        command: ['tkati-node', node.handler],
        env:     baseEnv + extraEnv,
      } + (
        if std.objectHas(node.deploy, 'resources')
        then { resources: [node.deploy.resources] }
        else {}
      );
      {
        metadata: [{ name: kebabName, namespace: k8s_namespace }],
        spec: [{
          replicas: node.deploy.replicas,
          selector: [{ match_labels: { app: kebabName } }],
          template: [{
            metadata: [{ labels: { app: kebabName } }],
            spec:     [{ container: [container] }],
          }],
        }],
      }
    for name in std.objectFields(manifest.nodes)
  };

  // --- Final Terraform JSON ---

  {
    resource: {
      kafka_topic:           kafkaTopicResources,
      kubernetes_deployment: k8sDeploymentResources,
    },
  }
