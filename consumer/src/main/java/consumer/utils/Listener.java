package consumer.utils;

import consumer.service.ConsumerService;

public class Listener extends Thread {

  private final ConsumerService service;

  public Listener(ConsumerService service) {
    this.service = service;
  }

  private void listen() {
    service.listen();
  }

  public void run() {
    listen();
  }
}
