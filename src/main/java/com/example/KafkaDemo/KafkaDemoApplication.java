package com.example.KafkaDemo;

import com.example.KafkaDemo.KafkaBasics.KafkaDemoProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class KafkaDemoApplication {


    public static void main(String[] args) {

        KafkaDemoProducer kafkaProducer = new KafkaDemoProducer();

        SpringApplication.run(KafkaDemoApplication.class, args);
    }

}
