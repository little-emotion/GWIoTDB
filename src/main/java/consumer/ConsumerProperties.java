package consumer;
//127.0.0.1
//10.12.20.72
public enum ConsumerProperties {
  ZK_URL("zookeeper.connect", "10.12.20.16:2181"), CONSUMER_GROUP_ID("group.id", "iotdb_consumers19"),
  TOPIC("topic", "all-tsdw-raw"), CONSUMER_THREAD_NUM("consumer_thread_num", 8), IOTDB_IP(
      "iotdb_ip", "127.0.0.1"), IOTDB_PORT("iotdb_port", 6667), IOTDB_USER("iotdb_user",
      "root"), IOTDB_PASSWARD("iotdb_passward", "root"), IOTDB_GROUP_NUM("iotdb_group_num", 8);

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
