package confirmation.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import confirmation.config.KafkaConfirmationConfig;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import producer.service.ProducerService;
import sbp.school.kafka.common.config.KafkaProperties;
import sbp.school.kafka.common.dto.ConfirmationDto;
import sbp.school.kafka.common.dto.TransactionDto;
import sbp.school.kafka.common.repository.TransactionRepository;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.nonNull;

public class ConfirmationService  {
  private final KafkaConsumer<String, ConfirmationDto> consumer;
  private final ProducerService producerService;
  private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

  public ConfirmationService() {
    consumer = KafkaConfirmationConfig.getConfoConsumer();
    this.producerService = new ProducerService();
  }

  public void listen() {
    try {
      consumer.subscribe(
              Collections.singleton(
                      KafkaProperties.getConfoProperties().getProperty(KafkaProperties.CONFIRMATION_TOPIC)
              )
      );
      consumer.assignment().forEach(this::commit);

      while(true) {
        ConsumerRecords<String, ConfirmationDto> consumerRecords = consumer.poll(Duration.ofMillis(200));

        for(ConsumerRecord<String, ConfirmationDto> consumerRecord : consumerRecords) {
          System.out.printf("Mesasge recieved: \n" +
                          "Topic: %s, \n" +
                          "Offset: %s \n" +
                          "Partition: %s \n" +
                          "Message: %s",
                  consumerRecord.topic(),
                  consumerRecord.offset(),
                  consumerRecord.partition(),
                  new ObjectMapper().writeValueAsString(consumerRecord.value()));

          checkHashSum(consumerRecord.value().getCheckSum(), 
                  consumerRecord.value().getTimestamp(),
                  Long.parseLong(KafkaProperties.getConfoProperties().getProperty(KafkaProperties.CONFIRMATION_DELAY)));

          offsets.put(
                  new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                  new OffsetAndMetadata(consumerRecord.offset() + 1L,"metadata empty")
          );
        }
        consumer.commitAsync();
      }
    } catch (Exception e) {
      System.out.println("Error :" + e);
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

  private void checkHashSum(String checkSum, String timestamp, Long duration) {
    List<TransactionDto> transactionDtoList = TransactionRepository.getTransactionsByTimeDuration(timestamp, duration);

    if (!checkSum.equals(getCheckSum(transactionDtoList))) {
      transactionDtoList.forEach(
              transaction -> {
                try {
                System.out.println(new ObjectMapper().writeValueAsString(transaction));
              } catch (JsonProcessingException e) {
                  throw new RuntimeException();
              }
              producerService.sendTransaction(transaction);
              }
      );
  }
}

  private String getCheckSum(List<TransactionDto> transactionDtoList) {
    try {
      List<String> items = transactionDtoList.stream().map(TransactionDto::getId).toList();
      return DigestUtils.sha256Hex(String.join("",items));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
