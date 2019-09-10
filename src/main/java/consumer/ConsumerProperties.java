package consumer;

public enum ConsumerProperties {
  ZK_URL("zookeeper.connect", "127.0.0.1:2181"), CONSUMER_GROUP_ID("group.id", "iotdb_consumers"),
  IOTDB_URL("iotdb_url", "127.0.0.1:6667"), TOPIC("topic", "all-tsdw-raw"), CONSUMER_THREAD_NUM
      ("consumer_thread_num", 8);
  // TODO add IoTDB configs

  private String propertyName;
  private Object defaultValue;

  ConsumerProperties(String propertyName, Object defaultValue) {
    this.propertyName = propertyName;
    this.defaultValue = defaultValue;
  }

  public String getPropertyName() {
    return propertyName;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }
}
