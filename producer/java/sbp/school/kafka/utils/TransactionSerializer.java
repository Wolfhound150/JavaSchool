package sbp.school.kafka.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.utils.SchemaValidator;

/*Сериализатор для операции*/
public class TransactionSerializer implements Serializer<TransactionDto> {
  @Override
  public byte[] serialize(String s, TransactionDto dto) {
    if (dto == null) {
      throw new NullPointerException("Transaction can't be null");
    }

    ObjectMapper mapper = new ObjectMapper();

    try {
      String s1 = mapper.writeValueAsString(dto);
      SchemaValidator.validate(mapper.readTree(s1), this.getClass().getResourceAsStream("/dtoTransaction-scheme.json"));
      return s1.getBytes();
    } catch (JsonProcessingException e) {
      System.out.printf("Serialization TransactionDto exception" + e + "%n");
      throw new RuntimeException(e);
    }
  }
}
