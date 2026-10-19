package com.example.KafkaDemo.KafkaWikimediaProducer;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.launchdarkly.eventsource.EventSource;

/**
 *
 *
 *  */

public class WikiMediaProducer {

    public static void main(String[] args) throws StreamException, InterruptedException {

        String bootstrapServer = "localhost:9092";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServer);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//        Set safe producer configs (Kafka <= 2.8)
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

//        Compression

//        Setting some high throughput producer configs

        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

//        TODO:
        BackgroundEventHandler eventHandler = new WikiMediaChangeHandler(producer, topic);


        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        ConnectStrategy strategy = ConnectStrategy.http(URI.create(url))
                .headers(Headers.of(Map.of(
                        "User-Agent",
                        "KafkaWikimediaProducer/1.0 (your-real-email@example.com)",
                        "Accept",
                        "text/event-stream"
                )));

        EventSource.Builder builder2 = new EventSource.Builder(strategy);

        BackgroundEventSource backgroundEventSource =
                new BackgroundEventSource.Builder(eventHandler, builder2).build();

        System.out.println("Starting Wikimedia stream...");
        backgroundEventSource.start();

        TimeUnit.MINUTES.sleep(10);

//        Start the producer in another thread

        System.out.println("Starting Wikimedia EventSource...");

        System.out.println("EventSource.start() returned.");

//        we produce for 10 minutes and block the program until then >

        TimeUnit.MINUTES.sleep(10);
    }
}
