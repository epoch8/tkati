# 0.3.0

* Add shared `Producer` base class implemented by `KafkaProducer` and `ClickhouseProducer`
* `ClickhouseProducer` now supports `produce_pylist`, `flush`, and `close`, and
  can be used as `dlq_producer` for another `ClickhouseProducer`

# 0.2.0

* Initial implementation of ClickhouseProducer

# 0.1.0

* Initial implementation of KafkaConsumer and KafkaProducer
