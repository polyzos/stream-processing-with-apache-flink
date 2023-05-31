package io.streamingledger.datastream.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamingledger.models.Customer;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class CustomerSerdes extends AbstractDeserializationSchema<Customer> {
    private ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        mapper =  new ObjectMapper();
        super.open(context);
    }

    @Override
    public Customer deserialize(byte[] bytes) throws IOException {
        return mapper.readValue(bytes, Customer.class);
    }
}
