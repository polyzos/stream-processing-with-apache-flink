package io.streamingledger.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingUtils {
    private static final Logger logger
            = LoggerFactory.getLogger(StreamingUtils.class);

    public static <K,V> void handleMessage(KafkaProducer<K, V> producer, String topic, K key, V value) {
        var record = new ProducerRecord(topic, key, value);
        producer.send(record, (metadata, exception) -> {
            if (exception !=null) {
                logger.error("Error while producing: ", exception);
            } else {
//                logger.info("Successfully stored offset '{}': partition: {} - {}", metadata.offset(), metadata.partition(), metadata.topic());
            }
        });
    }

    public static <K, V> void closeProducer(KafkaProducer<K, V> producer) {
        producer.flush();
        producer.close();
    }
}
