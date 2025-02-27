package consumer.service;

import confirmation.service.ConfirmationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import sbp.school.kafka.common.dto.TransactionDto;
import sbp.school.kafka.common.enums.OperationType;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ConsumerServiceTest {
  private static final String TOPIC_NAME = "DEMO_EVENTS";

  private MockConsumer<String, TransactionDto> mCons;

  private ConsumerService consumerService;

  @Mock
  private ConfirmationService confirmService;

  @BeforeEach
  void setUp() {
    mCons = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    mCons.subscribe(Collections.singletonList(TOPIC_NAME));
    mCons.rebalance(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
    mCons.updateBeginningOffsets(Collections.singletonMap(
            new TopicPartition(TOPIC_NAME, 0), 0L));

    consumerService = new ConsumerService("group-id", confirmService);
    consumerService.setConsumer(mCons);
  }

  @AfterEach
  void tearDown() {
    mCons.close();
  }
  @Test
  void listenTest() throws InterruptedException {
      TransactionDto dto = new TransactionDto(
              "111",
              OperationType.DEBIT,
              BigDecimal.TEN,
              "account",
              Calendar.getInstance()
      );

      mCons.addRecord(
              new ConsumerRecord<>(
                      TOPIC_NAME,
                      0,
                      0,
                      null,
                      dto
              )
      );

      CompletableFuture.runAsync(() -> consumerService.listen());
      Thread.sleep(100);
      mCons.schedulePollTask(() -> mCons.wakeup());

      verify(confirmService).sendConfo();
    }
  }
