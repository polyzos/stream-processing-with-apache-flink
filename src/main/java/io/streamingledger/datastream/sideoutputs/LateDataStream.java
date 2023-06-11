package io.streamingledger.datastream.sideoutputs;

import io.streamingledger.config.AppConfig;
import io.streamingledger.datastream.serdes.CustomerSerdes;
import io.streamingledger.datastream.serdes.TransactionSerdes;
import io.streamingledger.datastream.sideoutputs.handlers.LateDataHandler;
import io.streamingledger.models.Customer;
import io.streamingledger.models.Transaction;
import io.streamingledger.models.TransactionEnriched;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class LateDataStream {
    public static void main(String[] args) throws Exception {
        // 1. Initialize the execution environment
        var environment = StreamExecutionEnvironment
                .createLocalEnvironmentWithWebUI(new Configuration());

        environment.setParallelism(1);


        // 3. Initialize Transactions Source
        KafkaSource<Transaction> txnSource = KafkaSource.<Transaction>builder()
                .setBootstrapServers(AppConfig.BOOTSTRAP_URL)
                .setTopics(AppConfig.TRANSACTIONS_TOPIC)
                .setGroupId("group.finance.transactions")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TransactionSerdes())
                .build();


        WatermarkStrategy<Transaction> watermarkStrategy = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((txn, timestamp) -> txn.getEventTime());

        DataStream<Transaction> transactionStream = environment
                .fromSource(txnSource, watermarkStrategy, "Transactions Source")
                .setParallelism(5)
                .name("TransactionSource")
                .uid("TransactionSource");

        final OutputTag<Transaction> missingStateTag
                = new OutputTag<>("missingState"){};

        SingleOutputStreamOperator<Transaction> enrichedStream =
                transactionStream
                        .process(new LateDataHandler(missingStateTag))
                        .uid("EnrichmentHandler")
                        .name("EnrichmentHandler");

        DataStream<Transaction> missingStateStream =
                enrichedStream
                        .getSideOutput(missingStateTag);
        missingStateStream
                .print()
                .uid("missingStatePrint")
                .name("missingStatePrint");

        environment.execute("Data Enrichment Stream - Missing State and Side Outputs");
    }
}
