package io.streamingledger.producers;

import io.streamingledger.config.AppConfig;
import io.streamingledger.models.Account;
import io.streamingledger.models.Customer;
import io.streamingledger.utils.DataSourceUtils;
import io.streamingledger.utils.StreamingUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

import static io.streamingledger.utils.StreamingUtils.closeProducer;

public class StateProducer {
    private static final Logger logger
            = LoggerFactory.getLogger(StateProducer.class);

    public static void main(String[] args) throws IOException {
        Stream<Customer> customers = DataSourceUtils
                .loadDataFile("/data/customers.csv")
                .map(DataSourceUtils::toCustomer);

        Stream<Account> accounts = DataSourceUtils
                .loadDataFile("/data/accounts.csv")
                .map(DataSourceUtils::toAccount);

        var properties = AppConfig.buildProducerProps();
        logger.info("Starting Kafka Producers  ...");

        var customersProducer = new KafkaProducer<String, Customer>(properties);
        var accountsProducer = new KafkaProducer<String, Account>(properties);

        logger.info("Generating customer information ...");

        var count = 0;
        for (Iterator<Customer> it = customers.iterator(); it.hasNext(); ) {
            Customer customer = it.next();
            StreamingUtils.handleMessage(customersProducer, AppConfig.CUSTOMERS_TOPIC, customer.getCustomerId(), customer);
            count += 1;
        }

        logger.info("Total customers sent '{}'.", count);

        logger.info("Generating accounts information ...");

        count = 0;
        for (Iterator<Account> it = accounts.iterator(); it.hasNext(); ) {
            Account account = it.next();
            StreamingUtils.handleMessage(accountsProducer, AppConfig.ACCOUNTS_TOPIC, account.getAccountId(), account);
            count += 1;
        }

        logger.info("Total accounts sent '{}'.", count);
        logger.info("Closing Producers ...");
        closeProducer(accountsProducer);
        closeProducer(customersProducer);
    }
}
