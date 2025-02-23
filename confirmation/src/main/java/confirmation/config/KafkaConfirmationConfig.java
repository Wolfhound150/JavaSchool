package confirmation.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import sbp.school.kafka.common.config.KafkaProperties;
import sbp.school.kafka.common.dto.ConfirmationDto;

import java.util.Properties;

public class KafkaConfirmationConfig {

  public static KafkaConsumer<String, ConfirmationDto> getConfoConsumer() {
    Properties prop = KafkaProperties.getConfoConsumerProperties();
    /*определяем имя группы на основании кастомного имени свойства*/
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, prop.getProperty(KafkaProperties.CONFIRMATION_GROUP));
    return new KafkaConsumer<>(prop);
  }

  public static KafkaProducer<String, ConfirmationDto> GetConfoProducer() {
    Properties prop = KafkaProperties.getConfoProducerProperties();
    return new KafkaProducer<>(prop);
  }
}
