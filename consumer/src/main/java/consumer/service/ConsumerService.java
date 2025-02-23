package consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import confirmation.service.ConfirmationService;
import consumer.config.KafkaConsumerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import sbp.school.kafka.common.config.KafkaProperties;
import sbp.school.kafka.common.dto.TransactionDto;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.nonNull;

public class ConsumerService {
  private Consumer<String, TransactionDto> consumer;
  private final ConfirmationService confirm;
  private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

  public ConsumerService(String groupId, ConfirmationService confirm) {
    this.consumer = KafkaConsumerConfig.getKafkaConsumer(groupId);
    this.confirm = new ConfirmationService(groupId);

  }

  public void listen() {
    try {
      String topicName = KafkaProperties.getTransactionTopic();

      consumer.subscribe(Collections.singleton(topicName));
      consumer.assignment().forEach(this::commit);

      while (true) {
        ConsumerRecords<String, TransactionDto> records = consumer.poll(Duration.ofMillis(200));

        for (ConsumerRecord<String, TransactionDto> record: records) {
          System.out.printf("Mesasge recieved: \n" +
                  "Topic: %s, \n" +
                  "Offset: %s \n" +
                  "Partition: %s \n" +
                  "Message: %s",
                  record.topic(),
                  record.offset(),
                  record.partition(),
                  new ObjectMapper().writeValueAsString(record.value()));
          offsets.put(
                  new TopicPartition(record.topic(), record.partition()),
                  new OffsetAndMetadata(record.offset() + 1L,"metadata empty")
          );
        }
        confirm.sendConfo();
        consumer.commitAsync();
      }
    } catch (Exception e) {
      System.out.println("Unexpected error " + e);
    } finally {
      try {
        consumer.commitSync();
      } finally {
        consumer.close();
      }
    }
  }

  private void commit(TopicPartition prtn) {
    var offsetAndMetadata = offsets.get(prtn);
    if (nonNull(offsetAndMetadata)) {
      consumer.seek(prtn, offsetAndMetadata);
    }
  }

  public void setConsumer(Consumer<String, TransactionDto> consumer) {
    this.consumer = consumer;
  }
}
