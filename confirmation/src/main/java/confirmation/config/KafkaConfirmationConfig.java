package confirmation.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import sbp.school.kafka.common.config.KafkaProperties;
import sbp.school.kafka.common.dto.ConfirmationDto;

import java.util.Properties;

public class KafkaConfirmationConfig {

  public static Consumer<String, ConfirmationDto> getConfoConsumer(String groupId) {
    Properties prop = KafkaProperties.getConfoConsumerProperties();
    /*определяем имя группы на основании кастомного имени свойства, если не определили вручную*/
    if (groupId.equals("")) {
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, prop.getProperty(KafkaProperties.CONFIRMATION_GROUP));}
    else {
      prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }
    return new KafkaConsumer<>(prop);
  }

  public static Producer<String, ConfirmationDto> GetConfoProducer() {
    Properties prop = KafkaProperties.getConfoProducerProperties();
    return new KafkaProducer<>(prop);
  }
}
