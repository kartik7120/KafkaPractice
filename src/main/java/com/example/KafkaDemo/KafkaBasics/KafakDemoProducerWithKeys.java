package com.example.KafkaDemo.KafkaBasics;

import com.example.KafkaDemo.KafkaDemoApplication;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafakDemoProducerWithKeys {

    private static final Logger log = LoggerFactory.getLogger(KafkaDemoApplication.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

//        Connect to localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");

//        Set Producer Properties
//        Used to selerize the key and value of the message
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//        NOT RECOMMENDED IN PRODUCTION !!!
//        properties.setProperty("batch.size", "400");
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

//        Create a Kafka Producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        Create a producer record / message that is sent to Kafka

        for (int i = 0; i < 2; i++) {

            for (int j = 0; j < 10; j++) {
                String topic = "new_topic";
                String key = "id_" + j;
                String value = "Hello World";

                //        Before sending data to a topic , we first need to create a topic in Kafka using CLI
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        topic, key, value
                );

//        Send data - asynchronus process


                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                This call is executed every time a record successfully send or an exception is thrown

                        if (e == null) {
//                  If no exception then that means that record was successfully send
                            log.info("Recieved Metadata \n" + "Key: " + key + "\n" + "Partition: " + recordMetadata.partition() + "\n");
                        } else if (e != null) {
                            log.error(e.getMessage());
                        }
                    }
                });
            }

            Thread.sleep(500);
        }


//        tell the producer to send all data and block until done - Synchronus process
        producer.flush();
//        Flush and close the producer
        producer.close();

        log.info("I am a Kafka Producer with callback");
    }

}
