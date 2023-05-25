package io.streamingledger.sql;

public class Queries {
    public static final String CREATE_TXN_TABLE = "CREATE TABLE transactions (\n" +
            "    transactionId      STRING,\n" +
            "    accountId          STRING,\n" +
            "    customerId         STRING,\n" +
            "    eventTime          BIGINT,\n" +
            "    eventTime_ltz AS TO_TIMESTAMP_LTZ(eventTime, 3),\n" +
            "    eventTimeFormatted STRING,\n" +
            "    type               STRING,\n" +
            "    operation          STRING,\n" +
            "    amount             DOUBLE,\n" +
            "    balance            DOUBLE,\n" +
            "        WATERMARK FOR eventTime_ltz AS eventTime_ltz\n" +
            ") WITH (\n" +
            "    'connector' = 'kafka',\n" +
            "    'topic' = 'transactions',\n" +
            "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
            "    'properties.group.id' = 'group.transactions',\n" +
            "    'format' = 'json',\n" +
            "    'scan.startup.mode' = 'earliest-offset'\n" +
            ");";

    public static final String TXN_DEDUPLICATION_QUERY = "SELECT transactionId, rowNum\n" +
            "FROM (\n" +
            "         SELECT *,\n" +
            "                ROW_NUMBER() OVER (PARTITION BY transactionId ORDER BY eventTime_ltz) AS rowNum\n" +
            "         FROM transactions)\n" +
            "WHERE rowNum = 1;";
}
