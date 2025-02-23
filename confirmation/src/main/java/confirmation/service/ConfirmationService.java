package confirmation.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import confirmation.config.KafkaConfirmationConfig;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import producer.service.ProducerService;
import sbp.school.kafka.common.config.KafkaProperties;
import sbp.school.kafka.common.dto.ConfirmationDto;
import sbp.school.kafka.common.dto.TransactionDto;
import sbp.school.kafka.common.repository.TransactionRepository;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.nonNull;

public class ConfirmationService  {
  private Consumer<String, ConfirmationDto> consumer;
  private Producer<String, ConfirmationDto> producer;
  private final ProducerService producerService;
  private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

  public ConfirmationService(String groupId) {
    consumer = KafkaConfirmationConfig.getConfoConsumer(groupId);
    this.producerService = new ProducerService();
    producer = KafkaConfirmationConfig.GetConfoProducer();
    try {
      TransactionRepository.createTable();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void listen() {
    try {
      consumer.subscribe(
              Collections.singleton(
                      KafkaProperties.getConfoConsumerProperties().getProperty(KafkaProperties.CONFIRMATION_TOPIC)
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
                  Long.parseLong(KafkaProperties.getConfoConsumerProperties().getProperty(KafkaProperties.CONFIRMATION_DELAY)));

          offsets.put(
                  new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                  new OffsetAndMetadata(consumerRecord.offset() + 1L,"metadata empty")
          );
        }
        consumer.commitAsync();
      }
    } catch (Exception e) {
      System.out.println("Processing Confirmation message error :" + e);
    } finally {
      try {
        consumer.commitSync();
      } finally {
        consumer.close();
      }

    }
  }

  public void sendConfo() {
    String ts = Timestamp.from(
            Instant.now().minus(Duration.ofMinutes(
                    Long.parseLong(
                            KafkaProperties.getConfoProducerProperties().getProperty(KafkaProperties.CONFIRMATION_GAP)
                    )
            ))
    ).toString();
    List<TransactionDto> transDto =
            TransactionRepository.getTransactionsByTimeDuration(ts,
                    Long.valueOf(KafkaProperties.getConfoConsumerProperties().getProperty(KafkaProperties.CONFIRMATION_DELAY)));

    ConfirmationDto confo = new ConfirmationDto(ts, getCheckSum(transDto));

    try {
      producer.send(
              new ProducerRecord<>(KafkaProperties.getConfoConsumerProperties().getProperty(KafkaProperties.CONFIRMATION_TOPIC), confo),
              ((recordMetadata, e) -> callback(recordMetadata, e, confo))
      );
    } finally {
      producer.flush(); /*сливаем, что бы не ждать пачку*/
    }

  }

  private void callback(RecordMetadata metadata, Exception e, ConfirmationDto dto) {
    if (e!=null) {
      System.out.printf("Sending message error! Offset: %s, Partition: %s, Error: %s%n",
              metadata.offset(),
              metadata.partition(),
              e.getMessage());
    } else
    {
      System.out.println("Sending message success: " + dto);}
  }

  private void commit(TopicPartition prtn) {
    var offsetAndMetadata = offsets.get(prtn);
    if (nonNull(offsetAndMetadata)) {
      consumer.seek(prtn, offsetAndMetadata);
    }
  }

/*сверка списка с сохраненным дайджестом*/
  private void checkHashSum(String checkSum, String timestamp, Long duration) {
    List<TransactionDto> transactionDtoList = TransactionRepository.getTransactionsByTimeDuration(timestamp, duration);

    if (!checkSum.equals(getCheckSum(transactionDtoList))) {
      transactionDtoList.forEach(
              transaction -> {
                try {
                System.out.println(new ObjectMapper().writeValueAsString(transaction));
                } catch (JsonProcessingException e) {
                  System.out.println("Mapping error in TransactionDto " + transaction);
                  throw new RuntimeException();
                }
              producerService.sendTransaction(transaction);
              }
      );
    } else {
      System.out.println("Confirmed transactions will be removed");
      TransactionRepository.deleteOldCompletedTransactions(timestamp, duration);
    }
}
/*ганератор дайджеста по списку транзакций*/
  public static String getCheckSum(List<TransactionDto> transactionDtoList) {
    try {
      List<String> items = transactionDtoList.stream().map(TransactionDto::getId).toList();
      return DigestUtils.sha256Hex(String.join("",items));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void setProducer(Producer<String, ConfirmationDto> producer) {
    this.producer = producer;
  }

  public void setConsumer(Consumer<String, ConfirmationDto> consumer) {
    this.consumer = consumer;
  }
}
