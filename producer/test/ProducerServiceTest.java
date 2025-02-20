import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.enums.OperationType;
import sbp.school.kafka.service.ProducerService;

import java.math.BigDecimal;
import java.util.Calendar;

class ProducerServiceTest {

  @Test
   void sendTransaction() {
    ProducerService service = new ProducerService();
    Assertions.assertDoesNotThrow(() -> service.sendTransaction(
            new TransactionDto(
                    OperationType.DEBIT,
                    BigDecimal.ONE,
                    "653653",
                    Calendar.getInstance()
            )
    ));
  }
}