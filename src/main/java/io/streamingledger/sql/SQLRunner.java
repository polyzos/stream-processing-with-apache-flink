package io.streamingledger.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SQLRunner {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();

        var environment = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(configuration);

        environment.setParallelism(5);

        var tableEnvironment = StreamTableEnvironment.create(environment);

        // Run some SQL queries to check the existing Catalogs, Databases and Tables
        tableEnvironment
                .executeSql("SHOW CATALOGS")
                .print();

        tableEnvironment
                .executeSql("SHOW DATABASES")
                .print();

        tableEnvironment
                .executeSql("SHOW TABLES")
                .print();

        tableEnvironment
                .executeSql(Queries.CREATE_TXN_TABLE)
                .print();

        tableEnvironment
                .executeSql("SHOW TABLES")
                .print();

        tableEnvironment
                .executeSql("DESCRIBE transactions")
                .print();

        tableEnvironment
                .executeSql(Queries.TXN_DEDUPLICATION_QUERY)
                .print();
    }
}
