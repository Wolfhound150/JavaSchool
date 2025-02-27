package sbp.school.kafka.common.dto;
/*подтверждающая транзакция*/
public class ConfirmationDto {

  private String timestamp;
  private String checkSum;

  public ConfirmationDto() {
  }

  public ConfirmationDto(String timestamp, String checkSum) {
    this.timestamp = timestamp;
    this.checkSum = checkSum;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public String getCheckSum() {
    return checkSum;
  }

  public void setCheckSum(String checkSum) {
    this.checkSum = checkSum;
  }
}
