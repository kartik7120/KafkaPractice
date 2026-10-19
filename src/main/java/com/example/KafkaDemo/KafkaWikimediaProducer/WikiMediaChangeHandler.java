package com.example.KafkaDemo.KafkaWikimediaProducer;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiMediaChangeHandler implements BackgroundEventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;
    private final static Logger log = LoggerFactory.getLogger(WikiMediaChangeHandler.class);

    public WikiMediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

//    @Override
//    public void onOpen() {
//        log.info("Connected to Wikimedia stream!");
//    }

//    @Override
//    public void onMessage(String event, MessageEvent messageEvent) {
//        String data = messageEvent.getData();
//
//        log.info("Message received: {}", data);
//
//        kafkaProducer.send(
//                new ProducerRecord<>(topic, data),
//                (metadata, exception) -> {
//                    if (exception != null) {
//                        log.error("Failed to send message to Kafka", exception);
//                    } else {
//                        log.info(
//                                "Sent to Kafka: topic={}, partition={}, offset={}",
//                                metadata.topic(),
//                                metadata.partition(),
//                                metadata.offset()
//                        );
//                    }
//                }
//        );
//    }

    @Override
    public void onOpen() {
        System.out.println("WIKIMEDIA CONNECTED");
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        System.out.println("MESSAGE RECEIVED: " + messageEvent.getData());

        kafkaProducer.send(
                new ProducerRecord<>(topic, messageEvent.getData())
        );
    }

    @Override
    public void onError(Throwable throwable) {
        System.err.println("WIKIMEDIA ERROR:");
        throwable.printStackTrace();
    }

    @Override
    public void onComment(String s) throws Exception {

    }

//    @Override
//    public void onError(Throwable throwable) {
//        log.error("Wikimedia stream error", throwable);
//    }

    @Override
    public void onClosed() {
        log.info("Wikimedia stream closed");
        kafkaProducer.close();
    }
}
