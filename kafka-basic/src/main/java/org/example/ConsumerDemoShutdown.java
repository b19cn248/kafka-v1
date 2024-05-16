package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoShutdown {

  private static final Logger log = LoggerFactory.getLogger(ConsumerDemoShutdown.class.getSimpleName());

  public static void main(String[] args) {

    log.info("Kafka Consumer Demo");

    String groupId = "my-first-application";
    String topic = "demo_java";

    // create Producer Properties
    Properties properties = new Properties();

    // connect to the Kafka cluster localhost:9092
    properties.setProperty("bootstrap.servers", "localhost:9092");

    //create consumer configs
    properties.setProperty("key.deserializer", StringDeserializer.class.getName());
    properties.setProperty("value.deserializer", StringDeserializer.class.getName());
    properties.setProperty("group.id", groupId);
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("auto.commit.interval.ms", "30000");
//    properties.setProperty("max.poll.interval.ms", "7000");

    // create the Producer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);


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

    try {
      // subscribe consumer to our topic(s)
      consumer.subscribe(Arrays.asList(topic));

      // poll for new data

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

        for (var record : records) {
          log.info("Key: " + record.key() + ", Value: " + record.value());
          log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
        }

//        Thread.sleep(6000);
      }
    } catch (WakeupException e) {
      log.error("Consumer is starting to shutdown", e);
    } catch (Exception e) {
      log.error("Unexpected exception in the consumer", e);
    } finally {
      consumer.close(); // close the consumer, this will also commit offsets
      log.info("Consumer is closing");
    }
  }
}
