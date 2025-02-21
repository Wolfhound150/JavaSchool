package sbp.school.kafka.dto;

import sbp.school.kafka.enums.OperationType;

import java.math.BigDecimal;
import java.util.Calendar;

/*Денежная транзакция*/
public class TransactionDto {

  private OperationType operType;
  private BigDecimal amount;
  private String account;
  private Calendar date;

  public TransactionDto() {
  }

  public TransactionDto(OperationType operType, BigDecimal amount, String account, Calendar date) {
    this.operType = operType;
    this.amount = amount;
    this.account = account;
    this.date = date;
  }

  public OperationType getOperType() {
    return operType;
  }

  public BigDecimal getAmount() {
    return amount;
  }

  public String getAccount() {
    return account;
  }

  public Calendar getDate() {
    return date;
  }

  public void setOperType(OperationType operType) {
    this.operType = operType;
  }

  public void setAmount(BigDecimal amount) {
    this.amount = amount;
  }

  public void setAccount(String account) {
    this.account = account;
  }

  public void setDate(Calendar date) {
    this.date = date;
  }
}
