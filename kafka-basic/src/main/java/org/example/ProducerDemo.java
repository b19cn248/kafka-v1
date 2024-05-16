package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

  private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

  public static void main(String[] args) throws InterruptedException {

    log.info("Hello world");

    // create Producer Properties
    Properties properties = new Properties();

    // connect to the Kafka cluster localhost:9092
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());


    properties.setProperty("enable.idempotence", "false");
    // Increase retries to simulate potential duplicate due to retries
    properties.setProperty("retries", "10");
    // Reduce the retry backoff to simulate retries more quickly
    properties.setProperty("retry.backoff.ms", "10");
    properties.setProperty("acks", "all");


    // create the Producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


    // create a Producer Record
    for (int i = 1; i <= 10; i++) {

      log.info("Producing record: " + i);

      ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test2", "Hello World " + i);

      // Send data asynchronously and use callback to log result or error
      producer.send(producerRecord, (metadata, exception) -> {
        if (exception != null) {
          log.error("Error while producing", exception);
        } else {
          log.info("Produced record to topic {} partition {} offset {}", metadata.topic(), metadata.partition(), metadata.offset());
        }
      });

      Thread.sleep(1000);
    }


    // send data
//    producer.send(producerRecord);

    // tell the producer to send all data and block until done -- sync
    producer.flush();

    // flush and close the producer
    producer.close();
  }
}
