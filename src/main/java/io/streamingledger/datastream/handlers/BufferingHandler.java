package io.streamingledger.datastream.handlers;

import io.streamingledger.models.Customer;
import io.streamingledger.models.Transaction;
import io.streamingledger.models.TransactionEnriched;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferingHandler
        extends CoProcessFunction<Transaction, Customer, TransactionEnriched> {
    private static final Logger logger = LoggerFactory.getLogger(BufferingHandler.class);
    private ValueState<Customer> customerState;
    private ValueState<Transaction> transactionState;

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("{}, initializing state ...", this.getClass().getSimpleName());

        customerState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<>(
                                "customerState",
                                Customer.class
                        )
                );

        transactionState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<>(
                                "transactionState",
                                Transaction.class
                        )
                );
    }

    @Override
    public void processElement1(Transaction transaction,
                                CoProcessFunction<Transaction, Customer,
                                        TransactionEnriched>.Context context,
                                Collector<TransactionEnriched> collector) throws Exception {
        TransactionEnriched enrichedEvent = new TransactionEnriched();
        enrichedEvent.setTransaction(transaction);

        Customer customer = customerState.value();
        if (customer == null) {
            logger.warn(
                    "Failed to find state for customer '{}' - buffering transaction.",
                    transaction.getAccountId()
            );
            transactionState.update(transaction);
        } else {
            enrichedEvent.setCustomer(customer);
        }
        collector.collect(enrichedEvent);
    }

    @Override
    public void processElement2(Customer customer,
                                CoProcessFunction<Transaction, Customer,
                                        TransactionEnriched>.Context context,
                                Collector<TransactionEnriched> collector) throws Exception {
        customerState.update(customer);

        // check if there is any transaction record waiting for a customer event to arrive
        Transaction transaction = transactionState.value();
        if (transaction != null) {
            logger.info(
                    "Found a buffering transaction and sending it downstream."
            );
            collector.collect(
                    new TransactionEnriched(transaction, customer)
            );

            // if there was a transaction we buffered, clear the state
            transactionState.clear();
        }
    }
}