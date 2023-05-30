package io.streamingledger.datastream.source;

import io.streamingledger.config.AppConfig;
import io.streamingledger.datastream.serdes.CustomerSerdes;
import io.streamingledger.models.Customer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class CustomerSource {
    public static void main(String[] args) throws Exception {
        // 1. Create the execution environment
        var environment = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());

        environment.setParallelism(1);

        // 2. Create the source
        KafkaSource<Customer> customerSource =
                KafkaSource.<Customer>builder()
                        .setBootstrapServers(AppConfig.BOOTSTRAP_URL)
                        .setTopics(AppConfig.CUSTOMERS_TOPIC)
                        .setGroupId("group.finance.transactions")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new CustomerSerdes())
                        .build();

        // 3. Create a watermark strategy
        WatermarkStrategy<Customer> watermarkStrategy =
                WatermarkStrategy
                        .<Customer>forBoundedOutOfOrderness(
                                Duration.ofSeconds(20)
                        )
                        .withTimestampAssigner(
                                (customer, timestamp) -> timestamp
                        );

        // 4. Create the stream
        DataStream<Customer> txnStream =
                environment
                        .fromSource(
                                customerSource,
                                watermarkStrategy,
                                "Customer Source"
                        )
                        .name("CustomerSource")
                        .uid("CustomerSource");

        // 5. Print it to the console
        txnStream
                .print()
                .uid("print")
                .name("print");

        // 6. Execute the program
        environment.execute("Customer Print Stream");
    }
}
