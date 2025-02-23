package confirmation.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import sbp.school.kafka.common.config.KafkaProperties;
import sbp.school.kafka.common.dto.ConfirmationDto;

import java.util.Properties;

public class KafkaConfirmationConfig {

  public static KafkaConsumer<String, ConfirmationDto> getConfoConsumer() {
    Properties prop = KafkaProperties.getConfoProperties();
    return new KafkaConsumer<>(prop);
  }
}
