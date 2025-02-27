package confirmation.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import confirmation.utils.ConfirmationSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import producer.service.ProducerService;
import sbp.school.kafka.common.dto.ConfirmationDto;
import sbp.school.kafka.common.dto.TransactionDto;
import sbp.school.kafka.common.repository.TransactionRepository;
import sbp.school.kafka.common.enums.OperationType;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

class ConfirmationServiceTest {

  private static final String TOPIC_NAME = "DEMO-EVENTS-CONFO";

  private MockProducer<String, ConfirmationDto> mProd;

  private MockConsumer<String, ConfirmationDto> mCons;

  private ConfirmationService confirmService;

  @Mock
  private ProducerService producerService;

  @Mock
  private ConfirmationSerializer confirmSerializer;

  @Mock
  private ObjectMapper objectMapper;

  @Mock
  private PreparedStatement preparedStatement;

  @org.junit.jupiter.api.BeforeEach
  void setUp() {
    mCons = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    mCons.subscribe(Collections.singletonList(TOPIC_NAME));
    mCons.rebalance(Collections.singletonList(new TopicPartition(TOPIC_NAME, 0)));
    mCons.updateBeginningOffsets(Collections.singletonMap(
            new TopicPartition(TOPIC_NAME, 0), 0L));

    confirmService = new ConfirmationService("group-id");
    confirmService.setConsumer(mCons);

    mProd = new MockProducer<>(
            true,
            new StringSerializer(),
            confirmSerializer
    );

    confirmService.setProducer(mProd);
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() {
    mCons.close();
    mProd.close();
  }

  @Test
  void listenTest() throws InterruptedException, ParseException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    Date parsedDate = dateFormat.parse("2025-02-23 00:00:00");
    Timestamp timestamp = new Timestamp(parsedDate.getTime());

    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(timestamp.getTime());

    TransactionDto transaction = new TransactionDto(
            "111",
            OperationType.DEBIT,
            BigDecimal.TEN,
            "account",
            calendar
    );

    ConfirmationDto dto = new ConfirmationDto("2025-02-23 00:00:00", ConfirmationService.getCheckSum(Collections.singletonList(transaction)));

    mCons.addRecord(
            new ConsumerRecord<>(
                    TOPIC_NAME,
                    0,
                    0,
                    null,
                    dto
            )
    );

    try (MockedStatic<TransactionRepository> repositoryMockedStatic = Mockito.mockStatic(TransactionRepository.class)) {
      repositoryMockedStatic.when(() -> TransactionRepository.getTransactionsByTimeDuration(timestamp.toString(), 60L))
              .thenReturn(Collections.singletonList(transaction));
    }

    CompletableFuture.runAsync(() -> confirmService.listen());
    Thread.sleep(100);
    mCons.schedulePollTask(() -> mCons.wakeup());

    verify(producerService).sendTransaction(eq(transaction));
  }
}