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

CREATE TEMPORARY VIEW txnWithCustomerInfo AS
SELECT
    transactionId,
    t.eventTime_ltz,
    type,
    amount,
    balance,
    fullName,
    email,
    address1
FROM transactions AS t
         JOIN customers AS c
              ON t.customerId = c.customerId;

CREATE TABLE debits (
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
    'topic' = 'transactions.debits',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'group.transactions.debits',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);


CREATE TABLE credits (
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
    'topic' = 'transactions.credits',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'group.transactions.credits',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);


SELECT
    d.transactionId AS debitId,
    d.customerId    AS debitCid,
    d.eventTime_ltz AS debitEventTime,
    c.transactionId AS creditId,
    c.customerId    AS creditCid,
    c.eventTime_ltz AS creditEventTime
FROM debits d, credits c
WHERE d.customerId = c.customerId
  AND d.eventTime_ltz BETWEEN c.eventTime_ltz - INTERVAL '1' HOUR AND c.eventTime_ltz;


SET 'table.exec.source.idle-timeout'='100';
SELECT
    transactionId,
    t.eventTime_ltz,
    TO_TIMESTAMP_LTZ(updateTime, 3) as updateTime,
    type,
    amount,
    balance,
    fullName,
    email,
    address1
FROM transactions AS t
    JOIN customers FOR SYSTEM_TIME AS OF t.eventTime_ltz AS c
ON t.customerId = c.customerId;




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
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'accounts',
    'username' = 'postgres',
    'password' = 'postgres'
);


SELECT
    transactionId,
    t.accountId,
    t.eventTime_ltz,
    TO_TIMESTAMP_LTZ(updateTime, 3) AS updateTime,
    type,
    amount,
    balance,
    districtId,
    frequency
FROM transactions AS t
         JOIN accounts FOR SYSTEM_TIME AS OF t.eventTime_ltz AS a
              ON t.accountId = a.accountId;