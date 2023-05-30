package io.streamingledger.datastream.multistreams.functions;

import io.streamingledger.models.Transaction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class RatioCalcFunc
        extends CoProcessFunction<Transaction, Transaction, String> {
    private int totalDebitsCount = 0;
    private int totalCreditsCount = 0;

    @Override
    public void processElement1(Transaction debits,
                                CoProcessFunction<Transaction, Transaction, String>.Context ctx,
                                Collector<String> out) throws Exception {
        totalDebitsCount += 1;
        double ratio =
                totalDebitsCount * 100.0 / (totalCreditsCount + totalDebitsCount);
        out.collect(String.format("Total debits ratio so far: %s", ratio));
    }

    @Override
    public void processElement2(Transaction credits,
                                CoProcessFunction<Transaction, Transaction, String>.Context ctx,
                                Collector<String> out) throws Exception {
        totalCreditsCount += 1;
        double ratio =
                totalCreditsCount * 100.0 / (totalCreditsCount + totalDebitsCount);
        out.collect(String.format("Total credits ratio so far: %s", ratio));
    }
}