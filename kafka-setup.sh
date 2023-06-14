docker exec -it kafka kafka-topics.sh \
    --create --topic accounts \
    --partitions 1 \
    --config cleanup.policy=compact,delete \
    --config retention.ms=600000 \
    --bootstrap-server localhost:9092

docker exec -it kafka kafka-topics.sh \
    --create --topic customers \
    --partitions 1 \
    --config cleanup.policy=compact,delete \
    --config retention.ms=600000 \
    --bootstrap-server localhost:9092

docker exec -it kafka kafka-topics.sh \
    --create --topic transactions \
    --partitions 5 \
    --bootstrap-server localhost:9092

docker exec -it kafka kafka-topics.sh \
    --create --topic transactions.credit \
    --partitions 5 \
    --bootstrap-server localhost:9092

docker exec -it kafka kafka-topics.sh \
    --create --topic transactions.debit \
    --partitions 5 \
    --bootstrap-server localhost:9092

docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

docker exec -it kafka kafka-topics.sh --describe --topic accounts --bootstrap-server localhost:9092
docker exec -it kafka kafka-topics.sh --describe --topic customers --bootstrap-server localhost:9092

docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092

docker exec -it kafka kafka-console-consumer.sh \
      --topic transactions \
      --from-beginning \
      --bootstrap-server localhost:9092