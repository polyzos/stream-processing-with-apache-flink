package io.streamingledger.config;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AppConfig {
    public static final String BOOTSTRAP_URL        = "localhost:19092";
    public static final String ACCOUNTS_TOPIC       = "accounts";
    public static final String CUSTOMERS_TOPIC      = "customers";
    public static final String TRANSACTIONS_TOPIC   = "transactions";
    public static final String DEBITS_TOPIC         = "transactions.debits";
    public static final String CREDITS_TOPIC        = "transactions.credits";

    public static Properties buildProducerProps() {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_URL);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        return properties;
    }

    public static Properties buildConsumerProps() {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_URL);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group.1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getCanonicalName());

        return properties;
    }
}
