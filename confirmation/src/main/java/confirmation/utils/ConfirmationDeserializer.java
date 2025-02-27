package confirmation.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.school.kafka.common.dto.TransactionDto;
import sbp.school.kafka.common.utils.SchemaValidator;

import java.io.IOException;

/*десериализатор для подтверждений*/
public class ConfirmationDeserializer implements Deserializer<TransactionDto> {
  private static final String TRANSACTION_JSON = "/dtoConfo-scheme.json";
  @Override
  public TransactionDto deserialize(String s, byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      throw new IllegalArgumentException("Deserialization error: byte arrays must not be null");
    }

    ObjectMapper mapper = new ObjectMapper();
    try {
      SchemaValidator.validate(mapper.readTree(bytes), getClass().getClassLoader().getResourceAsStream(TRANSACTION_JSON));
      return mapper.readValue(bytes, TransactionDto.class);
    } catch (IOException e) {
      System.out.println("Deserialization error to Dto" + e);
      throw new RuntimeException(e);
    }
  }
}
