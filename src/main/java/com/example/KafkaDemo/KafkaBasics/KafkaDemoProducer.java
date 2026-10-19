package com.example.KafkaDemo.KafkaBasics;

import com.example.KafkaDemo.KafkaDemoApplication;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Properties;

@NoArgsConstructor
public class KafkaDemoProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaDemoApplication.class.getSimpleName());

    public void createKafkaDemoProducer() {
        System.out.println("Creating Kafka Demo Producer");
    }

    public static void main(String[] args) {
        Properties properties = new Properties();

//        Connect to localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");

//        Set Producer Properties
//        Used to selerize the key and value of the message
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//        Create a Kafka Producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        Create a producer record / message that is sent to Kafka

//        Before sending data to a topic , we first need to create a topic in Kafka using CLI
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "hello world");

//        Send data - asynchronus process

        producer.send(record);

//        tell the producer to send all data and block until done - Synchronus process
        producer.flush();
//        Flush and close the producer
        producer.close();

        log.info("I am a Kafka Producer");
    }
}
