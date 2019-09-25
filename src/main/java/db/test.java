package db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class test {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);

  public static void main(String[] args) {
    for (int i = 0; i < 1000000; i++) {
      LOGGER.error("test log, i = {}",i);
    }
  }
}
