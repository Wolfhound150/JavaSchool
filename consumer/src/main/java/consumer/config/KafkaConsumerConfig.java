package consumer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import sbp.school.kafka.common.config.KafkaProperties;
import sbp.school.kafka.common.dto.TransactionDto;

import java.util.Properties;

public class KafkaConsumerConfig {

  public static KafkaConsumer<String, TransactionDto> getKafkaConsumer(String groupId) {
    Properties prop = KafkaProperties.getConsumerProperties();
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    return new KafkaConsumer<>(prop);
  }
}
