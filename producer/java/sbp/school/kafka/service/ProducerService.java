package sbp.school.kafka.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import sbp.school.kafka.config.KafkaConfig;
import sbp.school.kafka.config.PropertiesLoader;
import sbp.school.kafka.dto.TransactionDto;

public class ProducerService {

  private final KafkaProducer<String, TransactionDto> kafkaProducer;
  private final String topicName = PropertiesLoader.loadProperties("application.properties").getProperty("transaction.topic");

  public ProducerService() {
    kafkaProducer = KafkaConfig.getKafkaProducer();
  }

  public void sendTransaction(TransactionDto dto) {
    kafkaProducer.send(
            new ProducerRecord<>(topicName, dto.getType().name(), dto),
            (recordMetadata, e) -> callback(recordMetadata, e, dto)
    );
    kafkaProducer.flush();
  }

  private void callback(RecordMetadata metadata, Exception e, TransactionDto dto) {
    if (e!=null) {
      System.out.printf("Sending message error! Offset: %s, Partition: %s, Error: %s%n",
              metadata.offset(),
              metadata.partition(),
              e.getMessage());
    } else System.out.println("Sending message success: " + dto);
  }
}
