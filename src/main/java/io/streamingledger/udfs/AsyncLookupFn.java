package io.streamingledger.udfs;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

@FunctionHint(output = @DataTypeHint("ROW<serviceResponse STRING, responseTime INT>"))
public class AsyncLookupFn extends TableFunction<Row> {
    private static final Logger logger
            = LoggerFactory.getLogger(AsyncLookupFn.class);
    private static Random random;

    @Override
    public void open(FunctionContext context) throws Exception {
        logger.info("Starting Function {}.", AsyncLookupFn.class.getCanonicalName());
        random = new Random();
        super.open(context);
    }

    public void eval(String lookupKey) {
        logger.info("Performing lookup for key {}", lookupKey);
        List<CompletableFuture<Void>> futureList= new ArrayList<>();

        for (int i = 1; i <= 3; i ++ ) {
            int serviceId = i;
            var delay = random.nextInt(300);
            var future = CompletableFuture.runAsync(() -> {
                logger.info("Calling service " + serviceId);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                var response = "Service-" + serviceId + " response.";
                collect(Row.of(response, delay));

            });
            futureList.add(future);
        }
        futureList.forEach(CompletableFuture::join);
    }
}