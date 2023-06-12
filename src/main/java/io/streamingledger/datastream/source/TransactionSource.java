package io.streamingledger.datastream.source;

import io.streamingledger.config.AppConfig;
import io.streamingledger.datastream.serdes.TransactionSerdes;
import io.streamingledger.models.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class TransactionSource {
    public static void main(String[] args) throws Exception {
        // 1. Create the execution environment
        var environment = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());

        // 2. Create the source
        KafkaSource<Transaction> txnSource =
                KafkaSource.<Transaction>builder()
                        .setBootstrapServers(AppConfig.BOOTSTRAP_URL)
                        .setTopics(AppConfig.TRANSACTIONS_TOPIC)
                        .setGroupId("group.finance.transactions")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new TransactionSerdes())
                        .build();

        // 3. Create a watermark strategy
        WatermarkStrategy<Transaction> watermarkStrategy =
                WatermarkStrategy.<Transaction>
                                forBoundedOutOfOrderness(
                                        Duration.ofSeconds(5)
                        )
                .withTimestampAssigner(
                        (txn, timestamp) -> txn.getEventTime()
                );

        // 4. Create the stream
        DataStream<Transaction> txnStream = environment
                .fromSource(
                        txnSource,
                        watermarkStrategy,
                        "Transactions Source"
                )
                .setParallelism(5)
                .name("TransactionSource")
                .uid("TransactionSource");

        // 5. Print it to the console
        txnStream
                .print()
                .setParallelism(1)
                .uid("print")
                .name("print");

        // 6. Execute the program
        environment.execute("Data Enrichment Stream");
    }
}
