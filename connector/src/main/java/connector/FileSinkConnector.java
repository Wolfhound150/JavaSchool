package connector;

import config.FileSinkConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FileSinkConnector extends SinkConnector {

  private Map<String,String> confProps;
  public void start(Map<String, String> map) {
    confProps = map;
    System.out.println("FileSinkConnector will be started");
  }

  public Class<? extends Task> taskClass() {
    return FileSinkTask.class;
  }

  public List<Map<String, String>> taskConfigs(int i) {
    return Collections.nCopies(i, confProps);
  }

  public void stop() {
    System.out.println("FileSinkConnector will be stopped");
  }

  public ConfigDef config() {
    return FileSinkConfig.CONFIG_DEF;
  }

  public String version() {
    return ".01";
  }
}
