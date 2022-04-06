/home/cedric/bin/kafka_2.11-2.4.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic name_topic \
  --from-beginning \
  --formatter kafka.tools.DefaultMessageFormatter \
  --property print.key=true \
  --property print.value=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer