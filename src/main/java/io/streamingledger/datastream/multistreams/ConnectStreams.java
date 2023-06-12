package io.streamingledger.datastream.multistreams;

import io.streamingledger.config.AppConfig;
import io.streamingledger.datastream.multistreams.functions.RatioCalcFunc;
import io.streamingledger.datastream.serdes.TransactionSerdes;
import io.streamingledger.models.Transaction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class ConnectStreams {
    public static void main(String[] args) throws Exception {
        var environment = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());

        environment.setParallelism(1);

        KafkaSource<Transaction> creditSource =
                KafkaSource.<Transaction>builder()
                        .setBootstrapServers(AppConfig.BOOTSTRAP_URL)
                        .setTopics(AppConfig.CREDITS_TOPIC)
                        .setGroupId("group.finance.transactions.credits")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new TransactionSerdes())
                        .build();

        KafkaSource<Transaction> debitsSource =
                KafkaSource.<Transaction>builder()
                        .setBootstrapServers(AppConfig.BOOTSTRAP_URL)
                        .setTopics(AppConfig.DEBITS_TOPIC)
                        .setGroupId("group.finance.transactions.debits")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new TransactionSerdes())
                        .build();


        WatermarkStrategy<Transaction> watermarkStrategy =
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Transaction>) (txn, l) -> txn.getEventTime()
                        );


        DataStream<Transaction> creditStream =
                environment
                        .fromSource(
                                creditSource,
                                watermarkStrategy,
                                "Credits Source"
                        )
                        .name("CreditSource")
                        .uid("CreditSource");

        DataStream<Transaction> debitsStream =
                environment
                        .fromSource(
                                debitsSource,
                                watermarkStrategy,
                                "Debit Source"
                        )
                        .name("DebitSource")
                        .uid("DebitSource");


        creditStream
                .connect(debitsStream)
                .flatMap(new RatioCalcFunc())
                .print();

        environment.execute("Connected Streams");
    }
}
