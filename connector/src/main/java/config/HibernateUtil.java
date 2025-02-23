package config;

import entity.Transaction;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;

public class HibernateUtil {

  public static SessionFactory getSessionFactory(DBConfig config) {
    return new MetadataSources(
            new StandardServiceRegistryBuilder()
                    .applySetting("hibernate.connection.url", config.getString(DBConfig.DB_URL))
                    .applySetting("hibernate.connection.username", config.getString(DBConfig.DB_USER))
                    .applySetting("hibernate.connection.password", config.getString(DBConfig.DB_PASSWORD))
                    .applySetting("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect")
                    .build())
            .addAnnotatedClass(Transaction.class)
            .buildMetadata()
            .buildSessionFactory();
  }
}
