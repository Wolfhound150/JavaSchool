package connector;

import config.DBConfig;
import entity.Transaction;
import mapper.TransactionMapper;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static config.HibernateUtil.getSessionFactory;

public class DBSourceTask extends SourceTask {
  private SessionFactory sessionFactory;
  private long interval;
  private String lastProcessedId;
  private String tableName;
  private String topic;

  public String version() {
    return new DBSourceConnector().version();
  }

  public void start(Map<String, String> map) {
    System.out.println("DBSourceTask will be started");
    DBConfig config = new DBConfig(map);
    this.interval = config.getInterval();
    this.tableName = config.getTableName();
    this.topic = config.getTopic();
    sessionFactory = getSessionFactory(config);
    System.out.println("DB connected");
  }

  public List<SourceRecord> poll() throws InterruptedException {
    Thread.sleep(interval);

    try {
      Session session = sessionFactory.openSession();
      List<Transaction> list = session.createQuery(
                      "FROM Transaction where id >:lastId ORDER BY id", Transaction.class)
              .setParameter("lastId", lastProcessedId)
              .getResultList();
      if (list.isEmpty()) {
        return null;
      }

      List<SourceRecord> records = new ArrayList<SourceRecord>();
      for (Transaction trn : list) {
        records.add(new SourceRecord(
                sourcePrtn(),
                sourceOffset(trn.getId()),
                topic,
                null,
                TransactionMapper.toDto(trn)
        ));
        lastProcessedId = trn.getId();
      }
      return records;
    } finally {

    }
  }

  public void stop() {
    System.out.println("DBSourceTask will be stopped");
    if (sessionFactory != null) {
      sessionFactory.close();
    }
  }

  private Map<String,?> sourceOffset(String id) {
    return Collections.singletonMap("id", id);
  }

  private Map<String,?> sourcePrtn() {
    return Collections.singletonMap("table", tableName);
  }
}
