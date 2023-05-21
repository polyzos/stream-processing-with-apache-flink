FROM flink:latest
ADD jars/flink-sql-connector-kafka-1.17.0.jar /opt/flink/lib/
ADD jars/flink-connector-jdbc-3.1.0-1.17.jar /opt/flink/lib/
ADD jars/postgresql-42.6.0.jar /opt/flink/lib/
ADD jars/flink-sql-connector-postgres-cdc-2.3.0.jar /opt/flink/lib/