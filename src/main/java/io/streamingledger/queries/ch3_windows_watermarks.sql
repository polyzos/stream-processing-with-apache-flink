SET sql-client.execution.result-mode = tableau;

CREATE TABLE transactions (
    transactionId      STRING,
    accountId          STRING,
    customerId         STRING,
    eventTime          BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(eventTime, 3),
    eventTimeFormatted STRING,
    type               STRING,
    operation          STRING,
    amount             DOUBLE,
    balance            DOUBLE,
        WATERMARK FOR eventTime_ltz AS eventTime_ltz
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'group.transactions',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);


SELECT
    transactionId,
    eventTime_ltz
FROM transactions
LIMIT 10;


-- How many transactions per day?
SELECT
    window_start AS windowStart,
    window_end as windowEnd,
    COUNT(transactionId) as txnCount
FROM TABLE(
        TUMBLE(TABLE transactions, DESCRIPTOR(eventTime_ltz), INTERVAL '1' DAY)
    )
GROUP BY window_start, window_end;


SELECT
    window_start AS windowStart,
    window_end as windowEnd,
    COUNT(transactionId) as txnCount
FROM TABLE(
        HOP(TABLE transactions, DESCRIPTOR(eventTime_ltz), INTERVAL '2' HOUR, INTERVAL '1' DAY)
    )
GROUP BY window_start, window_end;

SELECT
    window_start AS windowStart,
    window_end as windowEnd,
    window_time AS windowTime,
    COUNT(transactionId) as txnCount
FROM TABLE(
        CUMULATE(TABLE transactions, DESCRIPTOR(eventTime_ltz), INTERVAL '2' HOUR, INTERVAL '1' DAY)
    )
GROUP BY window_start, window_end, window_time;


-- Top-3 Customers per week (max # of transactions)
SELECT *
FROM (
         SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY txnCount DESC) as rowNum
         FROM (
                  SELECT
                      customerId,
                      window_start,
                      window_end,
                      COUNT(transactionId) as txnCount
                  FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(eventTime_ltz), INTERVAL '7' DAY))
                  GROUP BY window_start, window_end, customerId
              )
     ) WHERE rowNum <= 3;