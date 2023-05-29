package io.streamingledger.datastream;

import io.streamingledger.config.AppConfig;
import io.streamingledger.datastream.handlers.BufferingHandler;
import io.streamingledger.datastream.serdes.CustomerSerdes;
import io.streamingledger.datastream.serdes.TransactionSerdes;
import io.streamingledger.models.Customer;
import io.streamingledger.models.Transaction;
import io.streamingledger.models.TransactionEnriched;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class BufferingStream {
    private static final String checkpointsDir  = String.format("file://%s/temp/checkpoints", System.getProperty("user.dir"));
    private static final String rocksDBStateDir  = String.format("file://%s/temp/state/rocksdb/", System.getProperty("user.dir"));

    private static final Logger logger
            = LoggerFactory.getLogger(BufferingStream.class);

    public static void main(String[] args) throws Exception {
        logger.info("Setting checkpoint path {} and state path {}.", checkpointsDir, rocksDBStateDir);
        var environment = StreamExecutionEnvironment
                .getExecutionEnvironment();

        // Checkpoint Configurations
        environment.enableCheckpointing(5000);
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        environment.getCheckpointConfig().setCheckpointStorage(checkpointsDir);

        var stateBackend = new EmbeddedRocksDBStateBackend();
        stateBackend.setDbStoragePath(rocksDBStateDir);
        environment.setStateBackend(stateBackend);

        environment.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // Configure Restart Strategy
        environment.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(5, Time.seconds(5))
        );

        // 2. Initialize Customer Source
        KafkaSource<Customer> customerSource = KafkaSource.<Customer>builder()
                .setBootstrapServers(AppConfig.BOOTSTRAP_URL_DOCKER)
                .setTopics(AppConfig.CUSTOMERS_TOPIC)
                .setGroupId("group.finance.customers")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CustomerSerdes())
                .build();

        DataStream<Customer> customerStream = environment
                .fromSource(customerSource, WatermarkStrategy.forMonotonousTimestamps(), "Customer Source")
                .name("CustomerSource")
                .uid("CustomerSource");


        // 3. Initialize Transactions Source
        KafkaSource<Transaction> txnSource = KafkaSource.<Transaction>builder()
                .setBootstrapServers(AppConfig.BOOTSTRAP_URL_DOCKER)
                .setTopics(AppConfig.TRANSACTIONS_TOPIC)
                .setGroupId("group.finance.transactions")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TransactionSerdes())
                .build();


        WatermarkStrategy<Transaction> watermarkStrategy = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((txn, timestamp) -> txn.getEventTime());

        DataStream<Transaction> transactionStream = environment
                .fromSource(txnSource, watermarkStrategy, "Transactions Source")
                .setParallelism(5)
                .name("TransactionSource")
                .uid("TransactionSource");

        // 6. Data Enrichment
        DataStream<TransactionEnriched> enrichedStream = transactionStream
                .keyBy(Transaction::getCustomerId)
                .connect(customerStream.keyBy(Customer::getCustomerId))
                .process(new BufferingHandler())
                .uid("CustomerLookup")
                .name("CustomerLookup");

        // 7. Print the results
        enrichedStream
                .print()
                .uid("print")
                .name("print");

        // 8. Execute the program
        environment.execute("Data Enrichment Stream");
    }
}
