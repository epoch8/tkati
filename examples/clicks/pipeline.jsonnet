function(env)
  local tk = import 'tkati.libsonnet';
  local clusters = env.clusters;
  local node_defaults = if std.objectHas(env, 'node_defaults') then env.node_defaults else {};
  local image = if std.objectHas(env, 'image') then env.image else { repo: 'my-registry/pipelines', tag: 'latest' };

  // schemas
  local raw_click = tk.schema('raw_click', fields=[
    { name: 'event_id', type: 'utf8' },
    { name: 'user_id', type: 'int64' },
    { name: 'url', type: 'utf8' },
    { name: 'ts', type: 'timestamp[ms, UTC]' },
  ]);

  local enriched_click = tk.schema('enriched_click', fields=[
    { name: 'event_id', type: 'utf8' },
    { name: 'user_id', type: 'int64' },
    { name: 'url', type: 'utf8' },
    { name: 'ts', type: 'timestamp[ms, UTC]' },
    { name: 'country', type: 'utf8' },
    { name: 'device', type: 'utf8' },
  ]);

  local clicks_hourly = tk.schema('clicks_hourly', fields=[
    { name: 'hour', type: 'timestamp[ms, UTC]' },
    { name: 'country', type: 'utf8' },
    { name: 'count', type: 'int64' },
  ]);

  // clusters come from the environment
  local prod = clusters.prod;

  // topics
  local clicks_raw = tk.kafka_topic(
    'clicks_raw',
    schema=raw_click,
    cluster=prod,
    partitions=12,
    encoding='json-per-message',
  );

  local clicks_enriched = tk.kafka_topic(
    'clicks_enriched',
    schema=enriched_click,
    cluster=prod,
    partitions=12,
    retention='30d',
    encoding='json-per-message',
  );

  local clicks_hourly_topic = tk.kafka_topic(
    'clicks_hourly',
    schema=clicks_hourly,
    cluster=prod,
    partitions=4,
    retention='90d',
    encoding='json-per-message',
  );

  // base deploy merged with environment-provided defaults
  local default_deploy = {
    image: image.repo + ':' + image.tag,
  } + node_defaults;

  tk.pipeline(nodes=[
    tk.node(
      'click_generator',
      inputs=[],
      outputs=tk.output(clicks_raw, key='user_id'),
      handler='pipelines.clicks.ClickGenerator',
      config={ batch_size: 50 },
      deploy=default_deploy,
    ),
    tk.node(
      'click_enricher',
      inputs=tk.input(clicks_raw, buffer_size=500, timeout='2s'),
      outputs=tk.output(clicks_enriched, key='user_id'),
      handler='pipelines.clicks.ClickEnricher',
      config={ geoip_db: '/data/GeoLite2-City.mmdb' },
      deploy=default_deploy,
    ),
    tk.node(
      'click_aggregator',
      inputs=clicks_enriched,
      outputs=clicks_hourly_topic,
      handler='pipelines.clicks.ClickAggregator',
      deploy=default_deploy,
    ),
  ])
