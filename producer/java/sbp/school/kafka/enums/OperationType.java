package sbp.school.kafka.enums;

public enum OperationType {
  DEBIT (0),
  CREDIT (1);

  private final int partNumber;
  OperationType(int partNumber) {
    this.partNumber = partNumber;
  }

  public int getPartNumber() {
    return partNumber;
  }
}
