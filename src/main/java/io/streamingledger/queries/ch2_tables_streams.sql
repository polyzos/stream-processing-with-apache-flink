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
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'group.transactions',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);

SELECT
    transactionId,
    eventTime_ltz,
    type,
    amount,
    balance
FROM transactions;

CREATE TABLE customers (
    customerId STRING,
    sex STRING,
    social STRING,
    fullName STRING,
    phone STRING,
    email STRING,
    address1 STRING,
    address2 STRING,
    city STRING,
    state STRING,
    zipcode STRING,
    districtId STRING,
    birthDate STRING,
    updateTime BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(updateTime, 3),
        WATERMARK FOR eventTime_ltz AS eventTime_ltz,
            PRIMARY KEY (customerId) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'customers',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'raw',
    'value.format' = 'json',
    'properties.group.id' = 'group.customers'
);

SELECT
    customerId,
    fullName,
    social,
    birthDate,
    updateTime
FROM customers;

CREATE TABLE accounts (
    accountId STRING,
    districtId INT,
    frequency STRING,
    creationDate STRING,
    updateTime BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(updateTime, 3),
        WATERMARK FOR eventTime_ltz AS eventTime_ltz,
            PRIMARY KEY (accountId) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'accounts',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'raw',
    'value.format' = 'json',
    'properties.group.id' = 'group.accounts'
);

SELECT *
FROM accounts
LIMIT 10;


SELECT
    transactionId,
    eventTime_ltz,
    type,
    amount,
    balance
FROM transactions
WHERE amount > 180000
    and type = 'Credit';


SELECT customerId, COUNT(transactionId) as txnPerCustomer
FROM transactions
GROUP BY customerId;

SELECT *
FROM (
         SELECT customerId, COUNT(transactionId) as txnPerCustomer
         FROM transactions
         GROUP BY customerId
     ) as e
WHERE txnPerCustomer > 300;

SELECT
    transactionId,
    eventTime_ltz,
    type,
    amount,
    balance
FROM transactions
ORDER BY eventTime_ltz;

CREATE TEMPORARY VIEW temp_premium AS
SELECT
    transactionId,
    eventTime_ltz,
    type,
    amount,
    balance
FROM transactions
WHERE amount > 180000
  and type = 'Credit';


SELECT
    transactionId,
    eventTime_ltz,
    convert_tz(cast(eventTime_ltz as string), 'Europe/London', 'UTC') AS eventTime_ltz_utc,
    type,
    amount,
    balance
FROM transactions
WHERE amount > 180000
  and type = 'Credit';


-- Deduplication
SELECT transactionId, rowNum
FROM (
         SELECT *,
                ROW_NUMBER() OVER (PARTITION BY transactionId ORDER BY eventTime_ltz) AS rowNum
         FROM transactions)
WHERE rowNum = 1;