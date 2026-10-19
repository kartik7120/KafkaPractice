package com.example.KafkaDemo.practice;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerPractice {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerPractice.class.getName());

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("group.id", "group1");

        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());

        props.setProperty("auto.offset.reset", "earliest");

        String topic = "test";
        String group = "group1";

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(topic));

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {

                log.info("Shutting down...");

                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {

            while (true) {

                log.info("Polling for messages");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord record : records) {

                    log.info(
                            """
                                    Key: "%s
                                    Value: "%s
                                    Partition: "%s
                                    Offset: "%s
                                    """.formatted(record.key(), record.value(), record.partition(), record.offset()));
                }
            }
        } catch (WakeupException e) {
            log.info("Shutting down Kafka Consumer");
        } catch (Exception e) {
            log.info("Unexpected error occurred.", e);
        } finally {
            consumer.close();
        }
    }
}
