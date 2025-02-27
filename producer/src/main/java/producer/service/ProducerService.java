package producer.service;

import producer.config.KafkaProducerConfig;
import sbp.school.kafka.common.config.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import sbp.school.kafka.common.dto.TransactionDto;
import sbp.school.kafka.common.repository.TransactionRepository;

import java.sql.SQLException;

public class ProducerService {

  private final KafkaProducer<String, TransactionDto> kafkaProducer;
  private final String topic;

  public ProducerService() {
    this.kafkaProducer = KafkaProducerConfig.getKafkaProducer();
    this.topic = KafkaProperties.getTransactionTopic();
    try {
      TransactionRepository.createTable();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void sendTransaction(TransactionDto dto) {
    kafkaProducer.send(
            new ProducerRecord<>(topic, dto.getOperType().name(), dto),
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
    } else
    {
      TransactionRepository.save(dto);
      System.out.println("Sending message success: " + dto);}
  }
}
