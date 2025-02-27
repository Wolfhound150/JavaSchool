package consumer.service;

import consumer.utils.Listener;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class ConsumerServiceTest {

  @Test
  void listen() {
    ExecutorService service = Executors.newFixedThreadPool(3);

    Assertions.assertDoesNotThrow(() -> {
      service.submit(new Listener(new ConsumerService("group-1")));
      service.submit(new Listener(new ConsumerService("group-2")));
      service.submit(new Listener(new ConsumerService("group-3")));
    });

  }
}