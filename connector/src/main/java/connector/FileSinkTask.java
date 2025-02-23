package connector;

import config.FileSinkConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import sbp.school.kafka.common.dto.TransactionDto;

import java.io.*;
import java.util.Collection;
import java.util.Map;

public class FileSinkTask extends SinkTask {

  private BufferedWriter bWriter;
  public String version() {
    return new FileSinkConnector().version();
  }

  public void start(Map<String, String> map) {
    System.out.println("FileSink Task will be started");

    FileSinkConfig config = new FileSinkConfig(map);
    String fPath = config.getFilePath();

    try {
      bWriter = new BufferedWriter(new OutputStreamWriter(
              new FileOutputStream(fPath, true)));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Exception with bufferedWriter", e);
    }

  }

  public void put(Collection<SinkRecord> collection) {
    try {
      for (SinkRecord record : collection) {
        TransactionDto dto = (TransactionDto) record.value();
        bWriter.write(toCsv(dto));
        bWriter.newLine();
      }
      bWriter.flush();
    } catch (IOException e) {
      System.out.println("Write file exception " + e.getMessage());
    }
  }

  private String toCsv(TransactionDto dto) {
    return String.format("%s,%s,%s,%s",
            dto.getOperType(),
            dto.getAccount(),
            dto.getAmount(),
            dto.getDate());
  }

  public void stop() {
    try {
      if (bWriter != null) bWriter.close();
    } catch (IOException e) {
      System.out.println("Exception while closing buffer " + e.getMessage());
    }
    System.out.println("FileSink Task will be stopped");
  }
}
