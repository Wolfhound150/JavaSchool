package producer.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import sbp.school.kafka.common.config.KafkaProperties;
import sbp.school.kafka.common.dto.TransactionDto;

import java.util.Properties;

public class KafkaProducerConfig {
  public static KafkaProducer<String, TransactionDto> getKafkaProducer() {
    Properties properties = KafkaProperties.getProducerProperties();
    return new KafkaProducer<>(properties);
  }
}
