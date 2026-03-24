{
  // Add an item to a keyed map, erroring on conflicting duplicates.
  local addUnique(acc, key, item, kind) =
    if std.objectHas(acc, key) then
      if acc[key] == item then acc
      else error 'Duplicate ' + kind + ' "' + key + '" with differing definition'
    else
      acc { [key]: item },

  // Qualified topic key used throughout the manifest.
  local topicKey(t) = t.cluster.name + '/' + t.name,

  // Normalize a single input value to a tk.input() object.
  local normalizeInputItem(x) =
    if x._type == 'input' then x
    else { _type: 'input', topic: x, buffer_size: null, timeout: null },

  // Normalize a single output value to a tk.output() object.
  local normalizeOutputItem(x) =
    if x._type == 'output' then x
    else { _type: 'output', topic: x, key: null },

  schema(name, fields):: {
    _type: 'schema',
    name: name,
    fields: fields,
  },

  kafka_cluster(name, brokers, defaults={}):: {
    _type: 'kafka_cluster',
    name: name,
    brokers: brokers,
    defaults: defaults,
  },

  kafka_topic(name, schema, cluster, partitions=null, retention=null, compacted=false, encoding='arrow-batch'):: (
    if schema._type != 'schema' then
      error 'kafka_topic "' + name + '": schema must be a tk.schema() object'
    else if cluster._type != 'kafka_cluster' then
      error 'kafka_topic "' + name + '": cluster must be a tk.kafka_cluster() object'
    else {
      _type: 'kafka_topic',
      name: name,
      schema: schema,
      cluster: cluster,
      partitions:
        if partitions != null then partitions
        else if std.objectHas(cluster.defaults, 'partitions') then cluster.defaults.partitions
        else 1,
      retention:
        if retention != null then retention
        else if std.objectHas(cluster.defaults, 'retention') then cluster.defaults.retention
        else '7d',
      compacted: compacted,
      encoding: encoding,
    }
  ),

  input(topic, buffer_size=null, timeout=null):: {
    _type: 'input',
    topic: topic,
    buffer_size: buffer_size,
    timeout: timeout,
  },

  output(topic, key=null):: {
    _type: 'output',
    topic: topic,
    key: key,
  },

  node(name, inputs, handler, outputs=[], config={}, deploy={}):: (
    if handler == '' then
      error 'node "' + name + '": handler must not be empty'
    else {
      _type: 'node',
      name: name,
      inputs:
        if std.isArray(inputs) then [normalizeInputItem(i) for i in inputs]
        else [normalizeInputItem(inputs)],
      outputs:
        if std.isArray(outputs) then [normalizeOutputItem(o) for o in outputs]
        else [normalizeOutputItem(outputs)],
      handler: handler,
      config: config,
      deploy: deploy,
    }
  ),

  pipeline(nodes)::
    // Collect all topic objects referenced by node inputs and outputs
    local allTopics = std.foldl(
      function(acc, node)
        acc
        + [i.topic for i in node.inputs]
        + [o.topic for o in node.outputs],
      nodes, []
    );

    // Deduplicate topics by cluster/name key
    local topicsByKey = std.foldl(
      function(acc, t) addUnique(acc, topicKey(t), t, 'kafka_topic'),
      allTopics, {}
    );

    // Collect clusters from topics, deduplicate by name
    local clustersByName = std.foldl(
      function(acc, t) addUnique(acc, t.cluster.name, t.cluster, 'kafka_cluster'),
      std.objectValues(topicsByKey), {}
    );

    // Collect schemas from topics, deduplicate by name
    local schemasByName = std.foldl(
      function(acc, t) addUnique(acc, t.schema.name, t.schema, 'schema'),
      std.objectValues(topicsByKey), {}
    );

    // Build compiled nodes map
    local nodesMap = std.foldl(
      function(acc, node)
        local _ = if !std.objectHas(node.deploy, 'image') then
          error 'node "' + node.name + '": deploy.image is required'
        else null;
        acc {
          [node.name]: {
            name: node.name,
            inputs: [
              { topic: topicKey(i.topic), buffer_size: i.buffer_size, timeout: i.timeout }
              for i in node.inputs
            ],
            outputs: [
              { topic: topicKey(o.topic), key: o.key }
              for o in node.outputs
            ],
            handler: node.handler,
            config:  node.config,
            deploy:  { replicas: 1 } + node.deploy,
          },
        },
      nodes, {}
    );

    // Build compiled kafka_topics map, keyed by cluster/name
    local kafkaTopicsMap = {
      [k]: {
        name:               topicsByKey[k].name,
        cluster:            topicsByKey[k].cluster.name,
        schema:             topicsByKey[k].schema.name,
        partitions:         topicsByKey[k].partitions,
        replication_factor:
          if std.objectHas(topicsByKey[k].cluster.defaults, 'replication_factor')
          then topicsByKey[k].cluster.defaults.replication_factor
          else 3,
        retention:          topicsByKey[k].retention,
        compacted:          topicsByKey[k].compacted,
        encoding:           topicsByKey[k].encoding,
      }
      for k in std.objectFields(topicsByKey)
    };

    // Build compiled kafka_clusters map
    local kafkaClustersMap = {
      [name]: {
        name:    clustersByName[name].name,
        brokers: clustersByName[name].brokers,
      }
      for name in std.objectFields(clustersByName)
    };

    // Build compiled schemas map
    local schemasMap = {
      [name]: {
        name:   schemasByName[name].name,
        fields: schemasByName[name].fields,
      }
      for name in std.objectFields(schemasByName)
    };

    {
      schemas:        schemasMap,
      kafka_clusters: kafkaClustersMap,
      kafka_topics:   kafkaTopicsMap,
      nodes:          nodesMap,
    },
}
