import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import producer.service.ProducerService;
import producer.utils.TransactionSerializer;
import sbp.school.kafka.common.dto.TransactionDto;
import sbp.school.kafka.common.enums.OperationType;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class ProducerServiceTest {
  private static final String TOPIC_NAME = "DEMO_EVENTS";

  private MockProducer<String, TransactionDto> mProd;
  private ProducerService producerService;
  @Mock
  private TransactionSerializer transactionSerializer;
  @BeforeEach
  void config() {
    mProd = new MockProducer<>(
            true,
            new StringSerializer(),
            transactionSerializer
    );

    producerService = new ProducerService();
    producerService.setProducer(mProd);
  }
@AfterEach
  void stop() {
    mProd.close();
  }

  @Test
   void sendTransaction() {
      TransactionDto trn = new TransactionDto(
        UUID.randomUUID().toString().replace("-", ""),
        OperationType.CREDIT,
        BigDecimal.ONE,
        "544531",
        Calendar.getInstance()
      );
    try {
      when(transactionSerializer.serialize(TOPIC_NAME, trn))
              .thenReturn(new ObjectMapper().writeValueAsBytes(trn));

      producerService.sendTransaction(trn);
      ProducerRecord<String,TransactionDto> record = mProd.history().get(1);
      assertEquals(record.partition(), 0);
      assertEquals(record.topic(), TOPIC_NAME);
      assertEquals(record.value(), trn);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

}