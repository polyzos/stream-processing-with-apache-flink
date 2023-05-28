SET sql-client.execution.result-mode = tableau;
SET sql-client.display.max-column-width = 50;

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
    UUID() AS cardNumber
FROM transactions
LIMIT 10;

SELECT
    transactionId,
    maskfn(UUID()) AS cardNumber
FROM transactions
LIMIT 10;

CREATE TEMPORARY VIEW sample AS
SELECT *
FROM transactions
LIMIT 10;


SELECT
    transactionId,
    operation,
    word,
    length
FROM sample, LATERAL TABLE(splitfn(operation));
