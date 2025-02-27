package confirmation.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import sbp.school.kafka.common.dto.ConfirmationDto;
import sbp.school.kafka.common.utils.SchemaValidator;

/*сериализатор для подтверждений*/
public class ConfirmationSerializer implements Serializer<ConfirmationDto> {

  // TODO надо бы сделать шаблон сериализтора
  @Override
  public byte[] serialize(String s, ConfirmationDto dto) {
    if (dto == null) {
      throw new NullPointerException("Transaction can't be null");
    }

    ObjectMapper mapper = new ObjectMapper();

    try {
      String s1 = mapper.writeValueAsString(dto);
      // TODO вынести схемы в общий модуль
      SchemaValidator.validate(mapper.readTree(s1), this.getClass().getResourceAsStream("/dtoConfo-scheme.json"));
      return s1.getBytes();
    } catch (JsonProcessingException e) {
      System.out.printf("Serialization ConfirmationDto exception" + e + "%n");
      throw new RuntimeException(e);
    }
  }
}
