package io.streamingledger.udfs;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaskingFn extends ScalarFunction {
    private static final Logger logger
            = LoggerFactory.getLogger(MaskingFn.class);

    @Override
    public void open(FunctionContext context) throws Exception {
        logger.info("Starting Function {}.", MaskingFn.class.getCanonicalName());
        super.open(context);
    }

    public String eval(String input) {
        var tokens = input.split("-");
        return "xxxx-xxxx-xxxx-" + tokens[tokens.length - 1].substring(0, 4);
    }
}
