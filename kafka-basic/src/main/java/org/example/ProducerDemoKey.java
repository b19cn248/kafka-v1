package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKey {

  private static final Logger log = LoggerFactory.getLogger(ProducerDemoKey.class.getSimpleName());

  public static void main(String[] args) {

    log.info("Hello world");

    // create Producer Properties
    Properties properties = new Properties();

    // connect to the Kafka cluster localhost:9092
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());
    properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());


    // create the Producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


    for (int i = 1; i < 15; i++) {

      String topic = "demo_java";
      String value = "Hello World " + i;
      // create a Producer Record
      ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, value);

      // send data
      producer.send(producerRecord, (recordMetadata, e) -> {
        // executes every time a record is successfully sent or an exception is thrown
        if (e == null) {
          // the record was successfully sent
          log.info("Value: " + value + "  |  Partition: " + recordMetadata.partition());
        } else {
          log.error("Error while producing", e);
        }
      });
    }


    // tell the producer to send all data and block until done -- sync
    producer.flush();

    // flush and close the producer
    producer.close();
  }
}
