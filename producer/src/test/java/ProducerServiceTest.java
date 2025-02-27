import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.common.dto.TransactionDto;
import sbp.school.kafka.common.enums.OperationType;
import producer.service.ProducerService;

import java.math.BigDecimal;
import java.util.Calendar;

class ProducerServiceTest {

  @Test
   void sendTransaction() {
    ProducerService service = new ProducerService();
    Assertions.assertDoesNotThrow(() -> service.sendTransaction(
            new TransactionDto(
                    OperationType.CREDIT,
                    BigDecimal.ONE,
                    "544531",
                    Calendar.getInstance()
            )
    ));
  }
}