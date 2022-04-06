# creation du topic
/home/cedric/bin/kafka_2.11-2.4.1/bin/kafka-topics.sh --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1\
        --partitions 1\
        --topic stream_topic