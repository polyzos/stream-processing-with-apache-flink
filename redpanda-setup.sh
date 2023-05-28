docker exec -it redpanda rpk \
    topic create accounts \
    -p 1 -c cleanup.policy=compact \
    --config retention.ms=600000

docker exec -it redpanda rpk \
    topic create customers  \
    -p 1 -c cleanup.policy=compact \
    --config retention.ms=600000

docker exec -it redpanda rpk \
    topic create \
    transactions.credit \
    -p 5

docker exec -it redpanda rpk \
    topic create \
    transactions.debit \
    -p 3

docker exec -it redpanda rpk \
    topic create transactions \
    -p 5