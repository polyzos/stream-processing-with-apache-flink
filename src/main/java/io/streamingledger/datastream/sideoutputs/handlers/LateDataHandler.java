package io.streamingledger.datastream.sideoutputs.handlers;


import io.streamingledger.models.Customer;
import io.streamingledger.models.Transaction;
import io.streamingledger.models.TransactionEnriched;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class LateDataHandler
        extends ProcessFunction<Transaction, Transaction> {
    private static final Logger logger = LoggerFactory.getLogger(LateDataHandler.class);
    private final OutputTag<Transaction> lateEventsOutputTag;
    public LateDataHandler(OutputTag<Transaction> lateEventsOutputTag) {

        this.lateEventsOutputTag = lateEventsOutputTag;
    }

    @Override
    public void processElement(Transaction transaction, ProcessFunction<Transaction, Transaction>.Context context, Collector<Transaction> collector) throws Exception {
        if (transaction.getEventTime() < context.timerService().currentWatermark()) {
            logger.warn("Timestamp: '{}' - Watermark: {}",
                    transaction.getEventTimeFormatted(),
                    new Timestamp(context.timerService().currentWatermark())
            );
            context.output(lateEventsOutputTag, transaction);
        }
    }
}