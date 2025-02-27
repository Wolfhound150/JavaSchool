package sbp.school.kafka.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import sbp.school.kafka.dto.TransactionDto;

import java.util.Properties;

public class KafkaConfig {
  public static KafkaProducer<String, TransactionDto> getKafkaProducer() {
    Properties properties = PropertiesLoader.loadProperties("application.properties");
    Properties kafkaProp = new Properties();

    kafkaProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    kafkaProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, properties.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    kafkaProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, properties.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    kafkaProp.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, properties.getProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG));
    kafkaProp.put(ProducerConfig.ACKS_CONFIG, properties.getProperty(ProducerConfig.ACKS_CONFIG));
    kafkaProp.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, properties.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG));

    return new KafkaProducer<>(kafkaProp);
  }
}
