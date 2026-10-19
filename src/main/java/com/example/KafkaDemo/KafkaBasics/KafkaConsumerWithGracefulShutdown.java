package com.example.KafkaDemo.KafkaBasics;

import com.example.KafkaDemo.KafkaDemoApplication;
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

public class KafkaConsumerWithGracefulShutdown {

    private static final Logger log = LoggerFactory.getLogger(KafkaDemoApplication.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");
        Properties properties = new Properties();

//        Connect to localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");
//        Create a consumer config

        String group_id = "my-java-application";
        String topic = "new_topic";

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", group_id);

        properties.setProperty("auto.offset.reset", "earliest");

//        Create a Kafka Consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


//        Subscribe to a topic

        consumer.subscribe(Arrays.asList(topic));

//        Get a reference to a main thread

        final Thread mainThread = Thread.currentThread();

//        Add the shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down Kafka Consumer, let's exit by calling consumer.wakeup()");
            consumer.wakeup(); // trigger an exeception in our consumer code and the exeception will be thrown on the consumer.poll line below, it will wait for the code to complete

//            join the main thread to allow execution of the code in the main thread.

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
            while (true) {

                log.info("Polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {

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
            log.error("Unexcepted exception in consumer", e);
        } finally {
            consumer.close(); // close the consumer and will also commit the consumer offsets
        }


    }
}
