package com.example.KafkaDemo.KafkaConsumerOpenSearch;

import com.google.gson.JsonParser;
import okhttp3.Credentials;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jspecify.annotations.NonNull;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class);

    public static void main(String[] args) throws IOException {

        HttpHost host = new HttpHost(
                "http",
                "localhost",
                9200
        );

        Properties properties = getProperties();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

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

        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(host)); consumer) {

            var response = client.info(RequestOptions.DEFAULT);

            System.out.println("Connected to OpenSearch!");
            System.out.println("Cluster: " + response.getClusterName());
            System.out.println("Version: " + response.getVersion());

            boolean indexExists = client.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {
                log.info("Index exists!");
            } else {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");

                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);

                log.info("Create Index Request: " + createIndexRequest.toString());
            }

            consumer.subscribe(Collections.singletonList("wikimedia.recentchange"));

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {

//                    Send the record into opensearch

//                    Strategy 1
//                    Define ID using Kafka Records

//                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();

//                    Strategy 2 where we extract ID from json

                    try {
                        String id = extractID(record.value());
                        IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);

//                        IndexResponse Indexresponse = client.index(indexRequest, RequestOptions.DEFAULT);

                        bulkRequest.add(indexRequest);

//                        log.info("Index Response: " + Indexresponse.getId());

                    } catch (Exception e) {

                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse blukResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted: " + blukResponse.getItems().length);

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

//                Commit offsets after a batch is consumed
                    consumer.commitSync();

                    log.info("Consumer Committed!");
                }


            }
        } catch (WakeupException e) {
            log.info("Shutting down Kafka Consumer");
        } catch (Exception e) {
            log.error("Unexcepted exception in consumer", e);
        } finally {
            consumer.close(); // close the consumer and will also commit the consumer offsets
//            client.close();
        }
    }

    private static String extractID(String value) {

        return JsonParser.parseString(value).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }

    private static @NonNull Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
//        Create a consumer config

        String group_id = "my-java-application";

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", group_id);

        properties.setProperty("auto.offset.reset", "earliest");

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return properties;
    }
//        Create our Kafka Client

//        Main code Logic

//        close things
}

