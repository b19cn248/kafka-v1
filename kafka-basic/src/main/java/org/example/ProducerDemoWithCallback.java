package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Hello world");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to the Kafka cluster localhost:9092
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");
        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());


        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 10; j++) {

            for (int i = 0; i < 30; i++) {
                // create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello World " + i);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            log.info("Received new metadata. \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        // create a Producer Record
//        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello World 1");

        // send data
//        producer.send(producerRecord, new Callback() {
//            @Override
//            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                // executes every time a record is successfully sent or an exception is thrown
//                if (e == null) {
//                    // the record was successfully sent
//                    log.info("Received new metadata. \n" +
//                            "Topic: " + recordMetadata.topic() + "\n" +
//                            "Partition: " + recordMetadata.partition() + "\n" +
//                            "Offset: " + recordMetadata.offset() + "\n" +
//                            "Timestamp: " + recordMetadata.timestamp());
//                } else {
//                    log.error("Error while producing", e);
//                }
//            }
//        });

        // tell the producer to send all data and block until done -- sync
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
