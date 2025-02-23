package sbp.school.kafka.common.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProperties {
  public static final String CONSUMER_PROPERTIES_FILE = "consumer.properties";
  public static final String PRODUCER_PROPERTIES_FILE = "producer.properties";
  public static final String COMMON_PROPERTIES_FILE = "common.properties";

  /*переменные под подтверждение транзакций*/
  public static final String CONFIRMATION_PROPERTIES_FILE = "confirmation.properties";
  public static final String CONFIRMATION_TOPIC = "confirm.transaction.topic";
  public static final String CONFIRMATION_DELAY = "confirm.check.timeout";


  public static Properties getProducerProperties() {
    Properties fileProps = PropertiesLoader.loadProperties(PRODUCER_PROPERTIES_FILE);
    Properties appProps = new Properties();
    loadProp(appProps, fileProps, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    loadProp(appProps, fileProps, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
    loadProp(appProps, fileProps, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
    loadProp(appProps, fileProps, ProducerConfig.PARTITIONER_CLASS_CONFIG);
    loadProp(appProps, fileProps, ProducerConfig.ACKS_CONFIG);
    loadProp(appProps, fileProps, ProducerConfig.COMPRESSION_TYPE_CONFIG);
    return appProps;
  }

  public static Properties getConsumerProperties() {
    Properties fileProps = PropertiesLoader.loadProperties(CONSUMER_PROPERTIES_FILE);
    Properties appProps = new Properties();
    loadProp(appProps, fileProps, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
    loadProp(appProps, fileProps, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
    loadProp(appProps, fileProps, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    return appProps;
  }

  public static String getTransactionTopic() {
      Properties fileProps = PropertiesLoader.loadProperties(COMMON_PROPERTIES_FILE);
      return fileProps.getProperty("transaction.topic");
  }

  private static void loadProp(Properties appProps, Properties fileProps, String prop) {
    String propValue = fileProps.getProperty(prop);
    System.out.println();
    appProps.put(prop, propValue);
  }

  public static Properties getConfoProperties() {
    Properties fileProps = PropertiesLoader.loadProperties(CONFIRMATION_PROPERTIES_FILE);
    Properties appProps = new Properties();
    loadProp(appProps, fileProps, ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
    loadProp(appProps, fileProps, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
    loadProp(appProps, fileProps, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    loadProp(appProps, fileProps, ConsumerConfig.GROUP_ID_CONFIG);
    loadProp(appProps, fileProps, CONFIRMATION_TOPIC);
    loadProp(appProps, fileProps, CONFIRMATION_DELAY);
    return appProps;
  }
}
