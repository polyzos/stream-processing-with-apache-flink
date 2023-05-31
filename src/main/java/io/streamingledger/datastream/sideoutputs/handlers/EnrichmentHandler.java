package io.streamingledger.datastream.sideoutputs.handlers;


import io.streamingledger.models.Customer;
import io.streamingledger.models.Transaction;
import io.streamingledger.models.TransactionEnriched;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichmentHandler
        extends CoProcessFunction<Transaction, Customer, TransactionEnriched> {
    private static final Logger logger = LoggerFactory.getLogger(EnrichmentHandler.class);
    private final OutputTag<TransactionEnriched> missingStateTag;
    private ValueState<Customer> customerState;

    public EnrichmentHandler(OutputTag<TransactionEnriched> missingStateTag) {
        this.missingStateTag = missingStateTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("{}, initializing state ...", this.getClass().getSimpleName());

        customerState = getRuntimeContext()
                .getState(
                        new ValueStateDescriptor<Customer>("customerState", Customer.class)
                );
    }

    @Override
    public void processElement1(Transaction transaction,
                                CoProcessFunction<Transaction, Customer, TransactionEnriched>.Context context,
                                Collector<TransactionEnriched> collector) throws Exception {
        TransactionEnriched enrichedEvent = new TransactionEnriched();
        enrichedEvent.setTransaction(transaction);
        Customer customer = customerState.value();

        if (customer == null) {
            logger.warn("Failed to find state for customer '{}'", transaction.getAccountId());
            context.output(missingStateTag, enrichedEvent);
        } else {
            enrichedEvent.setCustomer(customer);
            collector.collect(enrichedEvent);
        }
    }

    @Override
    public void processElement2(Customer customer,
                                CoProcessFunction<Transaction, Customer, TransactionEnriched>.Context context,
                                Collector<TransactionEnriched> collector) throws Exception {
        Thread.sleep(2);
        customerState.update(customer);
    }
}