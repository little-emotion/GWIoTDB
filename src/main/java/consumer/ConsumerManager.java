package consumer;


import static consumer.ConsumerProperties.CONSUMER_THREAD_NUM;
import static consumer.ConsumerProperties.IOTDB_GROUP_NUM;
import static consumer.ConsumerProperties.IOTDB_IP;
import static consumer.ConsumerProperties.IOTDB_PASSWARD;
import static consumer.ConsumerProperties.IOTDB_PORT;
import static consumer.ConsumerProperties.IOTDB_USER;
import static consumer.ConsumerProperties.TOPIC;

import db.IoTDB;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerManager {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerManager.class);

  private ConsumerConnector consumerConnector;
  private ExecutorService consumerPool;

  private int threadNum;
  private int groupNum;
  private String topic;

  private AtomicLong insertPointNum = new AtomicLong();
  private AtomicLong dropPointNum = new AtomicLong();

  private ConsumerManager(Properties properties) {

    properties.put("zookeeper.session.timeout.ms", "8000");
    properties.put("zookeeper.sync.internalTime.ms", "200");
    properties.put("auto.commit.interval.ms", "1000");
    properties.put("auto.offset.reset", "smallest");
    properties.put("auto.commit.enable", "true");
    properties.put("serializer.class", "kafka.serializer.StringEncoder");

    ConsumerConfig config = new ConsumerConfig(properties);
    consumerConnector = Consumer.createJavaConsumerConnector(config);
    threadNum = Integer.parseInt(properties
        .getProperty(CONSUMER_THREAD_NUM.getPropertyName(),
            CONSUMER_THREAD_NUM.getDefaultValue().toString()));
    topic = properties.getProperty(TOPIC.getPropertyName(),
        TOPIC.getDefaultValue().toString());

    consumerPool = Executors.newFixedThreadPool(threadNum);
  }

  private void consume() throws IoTDBSessionException {
    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(topic, threadNum);

    StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
    StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
    Map<String, List<KafkaStream<String, String>>> consumerMap = consumerConnector
        .createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
    List<KafkaStream<String, String>> streams = consumerMap.get(topic);
    groupNum = Integer.parseInt(IOTDB_GROUP_NUM.getDefaultValue().toString());
    IoTDB ioTDB = new IoTDB();
    ioTDB.registerStorageGroup(groupNum);
    ioTDB.closeSession();
    for (final KafkaStream<String, String> stream : streams) {
      consumerPool.submit(new ConsumeTask(stream, insertPointNum, dropPointNum));
    }
  }



  class ConsumeTask implements Runnable {

    private KafkaStream<String, String> stream;
    private IoTDB ioTDB;

    private ConsumeTask(KafkaStream<String, String> stream, AtomicLong pointNum, AtomicLong dropPointNum) {
      this.stream = stream;
      initDBConnection(pointNum, dropPointNum);
    }

    private void initDBConnection( AtomicLong pointNum,
        AtomicLong dropPointNum) {

      Session session = new Session(IOTDB_IP.getDefaultValue().toString(),
          IOTDB_PORT.getDefaultValue().toString(), IOTDB_USER.getDefaultValue().toString(),
          IOTDB_PASSWARD.getDefaultValue().toString());
      try {
        session.open();
        ioTDB = new IoTDB(session, pointNum, dropPointNum);
        ioTDB.setGroupNum(groupNum);
      } catch (IoTDBSessionException e) {
        logger.error("Cannot init connection:", e);
      }
    }

    public void run() {
      //get message
      for (MessageAndMetadata<String, String> message : stream) {
        String msg;
        try {
          msg = message.message();
          logger.debug("msg:{}", msg);
          JSONObject json = new JSONObject(msg);

          String table = json.get("tstable").toString();
          if (!table.equals("gw_scada_7s_extension")) {
            continue;
          }

          JSONObject tags = json.getJSONObject("tags");
          JSONObject fields = json.getJSONObject("fields");
          long timestamp = Long.parseLong(json.get("timestamp").toString());
          String wfid = tags.get("wfid").toString();
          String wtid = tags.get("wtid").toString();
          ioTDB.insert(wfid, wtid, timestamp, fields.toMap());
        } catch (Exception e) {
          if(!e.getMessage().contains("null")){
            logger.error("Receiving msg failed.", e);
          }
        }
      }
    }


  }

  public static void main(String[] args) throws IoTDBSessionException {
    // TODO read the properties, create a manager and run it
    Properties properties = loadProperties();
    if(args.length > 0){
      properties.put("iotdb_ip", args[0]);
    }
    ConsumerManager manager = new ConsumerManager(properties);
    manager.consume();

    long internalTimeIns = 6;
    new ScheduledThreadPoolExecutor(1)
        .scheduleAtFixedRate(manager.new LogThread(internalTimeIns), 0, internalTimeIns,
            TimeUnit.SECONDS);
  }

  class LogThread implements Runnable {

    long lastNum;
    long internalTime;

    LogThread(long internalTimeIns) {
      this.internalTime = internalTimeIns;
      this.lastNum = 0;
    }

    @Override
    public void run() {
      long nowNum = insertPointNum.get();
      logger.info(
          "Total success points number is {}, rate is {} points/s. Error points num is {}.",
        nowNum, (nowNum - lastNum) / internalTime, dropPointNum.get());
      lastNum = nowNum;

    }
  }


  private static Properties loadProperties() {
    Properties properties = new Properties();
    properties.put("zookeeper.connect", ConsumerProperties.ZK_URL.getDefaultValue());
    properties.put("group.id", ConsumerProperties.CONSUMER_GROUP_ID.getDefaultValue());
    return properties;
  }
}
