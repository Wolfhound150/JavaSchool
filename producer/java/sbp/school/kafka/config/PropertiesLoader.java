package sbp.school.kafka.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesLoader {
  public static Properties loadProperties(String propPath) {
    Properties prop = new Properties();

    try (InputStream stream = PropertiesLoader.class.getClassLoader().getResourceAsStream(propPath)) {
      prop.load(stream);
    } catch (IOException e) {
      System.out.println(String.format("Error in reading file %s", propPath) + e);
    }

    return prop;
  }
}
