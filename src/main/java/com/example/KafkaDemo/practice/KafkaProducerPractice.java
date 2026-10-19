package com.example.KafkaDemo.practice;

import com.example.KafkaDemo.KafkaBasics.ConsumerGroups;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Flow;

public class KafkaProducerPractice {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerPractice.class);

    public static void main(String[] args) {

        log.info("Starting the local Kafka Producer Test");

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("group.id", "test");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "test";
        String key = "t_key";
        String value = "t_value";

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        producer.send(record, new Callback() {

            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                if (e != null) {
                    log.error(e.getMessage());
                } else {
                    log.info("Successfully sent message to topic: " + record.topic());

                    log.info("""
                            
                            Key: %s,
                            Value: %s,
                            Offset: %s,
                            Timestamp: %s,
                            Partitioner: %s,
                            """.formatted(key, value, recordMetadata.offset(), recordMetadata.timestamp(), recordMetadata.partition()));
                }
            }
        });

        producer.flush();

        producer.close();
    }
}
