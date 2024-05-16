//package org.example;
//
//import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.apache.kafka.common.errors.RetriableException;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Properties;
//
//public class ProducerDemoDuplicateMessage {
//
//  private static final Logger log = LoggerFactory.getLogger(ProducerDemoDuplicateMessage.class.getSimpleName());
//
//  public static void main(String[] args) throws InterruptedException {
//
//    log.info("Hello world");
//
//    // create Producer Properties
//    Properties properties = new Properties();
//
//    // connect to the Kafka cluster localhost:9092
//    properties.setProperty("bootstrap.servers", "localhost:9092");
//    properties.setProperty("key.serializer", StringSerializer.class.getName());
//    properties.setProperty("value.serializer", StringSerializer.class.getName());
//
//
//    properties.setProperty("enable.idempotence", "false");
//    properties.setProperty("retries", "3");
//    properties.setProperty("retry.backoff.ms", );
//
//    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//    try {
//      for (int i = 0; i < 10; i++) {
//        // Gửi tin nhắn lần đầu
//        ProducerRecord<String, String> record = new ProducerRecord<>("test1", "key", Integer.toString(i));
//        producer.send(record, (RecordMetadata metadata, Exception e) -> {
//          if (e != null) {
//            System.out.println("Có lỗi xảy ra khi gửi tin nhắn: " + e.getMessage());
//          } else {
//            System.out.println("Tin nhắn gửi thành công: " + metadata.toString());
//          }
//        });
//
//        // Giả sử có sự cố mạng, gửi lại cùng một tin nhắn
//        if (i == 5) { // Chọn một chỉ số ngẫu nhiên để giả lập lỗi
//          throw new RetriableException("Giả lập lỗi tại i = 50");
//        }
//      }
//    } finally {
//      producer.close();
//    }
//  }
//}
