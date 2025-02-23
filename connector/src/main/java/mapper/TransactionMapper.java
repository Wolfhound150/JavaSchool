package mapper;

import entity.Transaction;

public class TransactionMapper {


  private TransactionMapper() {
  }

  public static Transaction toDto(Transaction entity) {
    return new Transaction()
            .setDate(entity.getDate())
            .setAmount(entity.getAmount())
            .setOperType(entity.getOperType())
            .setAccount(entity.getAccount());
  }
}
