package com.example;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

  public static RestHighLevelClient createOpenSearchClient() {
    String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

    // we build a URI from the connection string
    RestHighLevelClient restHighLevelClient;
    URI connUri = URI.create(connString);
    // extract login information if it exists
    String userInfo = connUri.getUserInfo();

    if (userInfo == null) {
      // REST client without security
      restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

    } else {
      // REST client with security
      String[] auth = userInfo.split(":");

      CredentialsProvider cp = new BasicCredentialsProvider();
      cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

      restHighLevelClient = new RestHighLevelClient(
            RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                  .setHttpClientConfigCallback(
                        httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                              .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


    }

    return restHighLevelClient;
  }

  public static void main(String[] args) throws IOException {


    Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());

    // first create an OpenSearch Client
    RestHighLevelClient openSearchClient = createOpenSearchClient();

    // create our Kafka Client
    KafkaConsumer<String, String> consumer = createKafkaConsumer();

    // get a refrence to the main thread
    final Thread mainThread = Thread.currentThread();

    // add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Detected a shutdown, let's exit by calling consumer.wakeup() ... ");
      consumer.wakeup();

      // join the main thread to allow the execution of the code in the main thread
      try {
        mainThread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }));


    //we need to create the index on OpenSearch if it doesn't exist already
    try (openSearchClient; consumer) {

      boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

      if (!indexExists) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
        openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        log.info("The Wikimedia Index has been created!");
      } else {
        log.info("The Wikimedia Index already exists!");
      }

      consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

      while (true) {

        ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(100));

        int recordCount = records.count();

        log.info("Received {} records", recordCount);

        BulkRequest bulkRequest = new BulkRequest();

        for (ConsumerRecord<String, String> record : records) {
          // insert data into OpenSearch

          try {
            IndexRequest indexRequest = new IndexRequest("wikimedia")
                  .source(record.value(), XContentType.JSON);

//            IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

            bulkRequest.add(indexRequest);

//            log.info("Inserted 1 document into Opersearch with id: {}", indexResponse.getId());
          } catch (Exception e) {
            log.error("Error inserting data into OpenSearch", e);
          }
        }

        if (bulkRequest.numberOfActions() > 0) {
          BulkResponse bulkItemResponses = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
          log.info("Inserted {} documents into OpenSearch", bulkItemResponses.getItems().length);

          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        // commit the offsets
        consumer.commitSync();
        log.info("Offsets have been committed");
      }

    } catch (WakeupException e) {
      log.error("Consumer is starting to shutdown", e);
    } catch (Exception e) {
      log.error("Unexpected exception in the consumer", e);
    } finally {
      consumer.close(); // close the consumer, this will also commit offsets
      openSearchClient.close();
      log.info("Consumer is closing");
    }

    // main code

    //close things
  }

  private static KafkaConsumer<String, String> createKafkaConsumer() {
    String groupId = "consumer-opensearch-demo";

    // create Producer Properties
    Properties properties = new Properties();

    // connect to the Kafka cluster localhost:9092
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("key.deserializer", StringDeserializer.class.getName());
    properties.setProperty("value.deserializer", StringDeserializer.class.getName());
    properties.setProperty("group.id", groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    return new KafkaConsumer<>(properties);
  }
}
