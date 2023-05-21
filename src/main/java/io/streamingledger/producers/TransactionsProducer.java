package io.streamingledger.producers;

import io.streamingledger.config.AppConfig;
import io.streamingledger.models.Transaction;
import io.streamingledger.utils.DataSourceUtils;
import io.streamingledger.utils.StreamingUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

import static io.streamingledger.utils.StreamingUtils.closeProducer;

public class TransactionsProducer {
    private static final Logger logger
            = LoggerFactory.getLogger(TransactionsProducer.class);
    private static final boolean slitTransactions = false;

    public static void main(String[] args) throws IOException, InterruptedException {
        Stream<Transaction> transactions = DataSourceUtils
                .loadDataFile("/data/transactions.csv")
                .map(DataSourceUtils::toTransaction);

        var properties = AppConfig.buildProducerProps();
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "64000");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        logger.info("Starting Kafka Producer  ...");

        var txnProducer = new KafkaProducer<String, Transaction>(properties);

        logger.info("Generating transactions ...");

        var count = 0;
        for (Iterator<Transaction> it = transactions.iterator(); it.hasNext(); ) {
            Transaction transaction = it.next();
            if (slitTransactions) {
                if (transaction.getType().equals("Credit")) {
                    StreamingUtils.handleMessage(txnProducer, AppConfig.DEBITS_TOPIC, transaction.getTransactionId(), transaction);
                } else {
                    StreamingUtils.handleMessage(txnProducer, AppConfig.CREDITS_TOPIC, transaction.getTransactionId(), transaction);

                }
            } else {
                StreamingUtils.handleMessage(txnProducer, AppConfig.TRANSACTIONS_TOPIC, transaction.getTransactionId(), transaction);
            }
            count += 1;
            if (count % 10000 == 0) {
                logger.info("Total so far {}.", count);
            }
        }

        logger.info("Total transactions sent '{}'.", count);

        logger.info("Closing Producers ...");
        closeProducer(txnProducer);
    }
}
