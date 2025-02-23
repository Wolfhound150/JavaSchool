package connector;

import config.DBConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DBSourceConnector extends SourceConnector {
  private Map<String,String> confProps;
  public void start(Map<String, String> map) {
    System.out.println("DBSourceConnector will be started");
    this.confProps = map;
  }

  public Class<? extends Task> taskClass() {
    return DBSourceTask.class;
  }

  public List<Map<String, String>> taskConfigs(int i) {
    return Collections.nCopies(i, confProps);
  }

  public void stop() {
    System.out.println("DBSourceConnector will be stopped");
  }

  public ConfigDef config() {
    return DBConfig.CONFIG_DEF;
  }

  public String version() {
    return ".01";
  }
}
