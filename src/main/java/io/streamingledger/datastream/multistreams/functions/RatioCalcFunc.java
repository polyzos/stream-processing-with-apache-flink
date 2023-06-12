package io.streamingledger.datastream.multistreams.functions;

import io.streamingledger.models.Transaction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class RatioCalcFunc
        extends RichCoFlatMapFunction<Transaction, Transaction, String> {
    private int totalDebitsCount = 0;
    private int totalCreditsCount = 0;


    @Override
    public void flatMap1(Transaction transaction, Collector<String> collector) throws Exception {
        totalDebitsCount += 1;
        double ratio =
                totalDebitsCount * 100.0 / (totalCreditsCount + totalDebitsCount);
        collector.collect(String.format("Total debits ratio so far: %s", ratio));
    }

    @Override
    public void flatMap2(Transaction transaction, Collector<String> collector) throws Exception {
        totalCreditsCount += 1;
        double ratio =
                totalCreditsCount * 100.0 / (totalCreditsCount + totalDebitsCount);
        collector.collect(String.format("Total credits ratio so far: %s", ratio));
    }
}