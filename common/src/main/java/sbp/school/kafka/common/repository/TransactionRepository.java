package sbp.school.kafka.common.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.h2.jdbcx.JdbcDataSource;
import sbp.school.kafka.common.dto.TransactionDto;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TransactionRepository {
  private static final String JDBC_URL = "jdbc:h2:../data/test";

  public static void createTable() throws SQLException {
    JdbcDataSource ds = new JdbcDataSource();
    ds.setURL(JDBC_URL);
    ds.setUser("sa");
    ds.setPassword("sa");
    Connection conn = ds.getConnection();
    Statement statement = conn.createStatement();
    statement.execute(
            "create table if not exists trn(" +
                    "id integer not null auto_increment, trn varchar(1000), date timestamp)"
    );
  }
  public static List<TransactionDto> getTransactionsByTimeDuration(String timestamp, Long duration) {
    List<TransactionDto> result = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();

    try {
      Connection conn = DriverManager.getConnection(JDBC_URL);
      PreparedStatement prepStat = conn.prepareStatement(
              "select * from transactions WHERE date " +
                      ">= cast (? as timestamp) - cast (? as interval second)"
      ); {
        prepStat.setTimestamp(1, Timestamp.valueOf(timestamp));
        prepStat.setLong(2, Integer.parseInt(String.valueOf(duration)));
        prepStat.execute();
        try (ResultSet rs = prepStat.getResultSet()) {
          result.add(mapper.readValue(rs.getString(2), TransactionDto.class));
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }
      } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public static void save(TransactionDto dto) {
    try {
      Connection conn = DriverManager.getConnection(JDBC_URL);
      PreparedStatement statement = conn.prepareStatement(
              "insert into transactions(trn, date) values (?,?) ");
      statement.setObject(1, new ObjectMapper().writeValueAsString(dto));
      statement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
      statement.execute();

    } catch (SQLException | JsonProcessingException e) {
      throw new RuntimeException(e);
    }

  }
/*удаление транзакций по дате&гэпу*/
  public static void deleteOldCompletedTransactions(String timestamp, Long duration) {
    try {
      Connection conn = DriverManager.getConnection(JDBC_URL);
      PreparedStatement statement = conn.prepareStatement(
              "delete from transactions WHERE date " +
               ">= cast (? as timestamp) - cast (? as interval second)"
      );
        statement.setTimestamp(1, Timestamp.valueOf(timestamp));
        statement.setLong(2, Integer.parseInt(String.valueOf(duration)));
        statement.execute();

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

