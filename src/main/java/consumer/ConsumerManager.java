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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerManager {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerManager.class);

  private Properties properties;
  private ConsumerConnector consumerConnector;
  private ExecutorService consumerPool;

  private int threadNum;
  private String topic;

  /**
   * outer map: key--wfid, inner map: key--wtid, value--set of names in fields.
   */
  private ConcurrentHashMap<String, ConcurrentHashMap<String, Set<String>>> schema;

  private final static int TRY_NUM = 3;

  private AtomicLong timeSeriesNum = new AtomicLong();
  private AtomicLong insertPointNum = new AtomicLong();
  private AtomicLong dropPointNum = new AtomicLong();

  public ConsumerManager(Properties properties) {
    this.properties = properties;

    properties.put("zookeeper.session.timeout.ms", "4000");
    properties.put("zookeeper.sync.internalTime.ms", "200");
    properties.put("auto.commit.interval.ms", "5000");
    properties.put("auto.offset.reset", "smallest");
    properties.put("auto.commit.enable", "false");
    properties.put("serializer.class", "kafka.serializer.StringEncoder");

    ConsumerConfig config = new ConsumerConfig(properties);
    consumerConnector = Consumer.createJavaConsumerConnector(config);
    threadNum = Integer.parseInt(properties
        .getProperty(CONSUMER_THREAD_NUM.getPropertyName(),
            CONSUMER_THREAD_NUM.getDefaultValue().toString()));
    topic = properties.getProperty(TOPIC.getPropertyName(),
        TOPIC.getDefaultValue().toString());

    consumerPool = Executors.newFixedThreadPool(threadNum);

    schema = new ConcurrentHashMap<>();
  }

  public void consume() {
    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(topic, threadNum);

    StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
    StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
    Map<String, List<KafkaStream<String, String>>> consumerMap = consumerConnector
        .createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
    List<KafkaStream<String, String>> streams = consumerMap.get(topic);

    for (final KafkaStream<String, String> stream : streams) {
      consumerPool.submit(new ConsumeTask(stream, timeSeriesNum, insertPointNum, dropPointNum));
    }
  }


  class ConsumeTask implements Runnable {

    private KafkaStream<String, String> stream;
    private IoTDB ioTDB;

    private ConsumeTask(KafkaStream<String, String> stream, AtomicLong timeSeriesNum,
        AtomicLong pointNum, AtomicLong dropPointNum) {
      this.stream = stream;
      initDBConnection(timeSeriesNum, pointNum, dropPointNum);
    }

    private void initDBConnection(AtomicLong timeSeriesNum, AtomicLong pointNum,
        AtomicLong dropPointNum) {

      Session session = new Session(IOTDB_IP.getDefaultValue().toString(),
          IOTDB_PORT.getDefaultValue().toString(), IOTDB_USER.getDefaultValue().toString(),
          IOTDB_PASSWARD.getDefaultValue().toString());
      try {
        session.open();

        ioTDB = new IoTDB(session, timeSeriesNum, pointNum, dropPointNum);
        int groupNum = Integer.parseInt(IOTDB_GROUP_NUM.getDefaultValue().toString());
        ioTDB.registerStorageGroup(groupNum);
      } catch (IoTDBSessionException e) {
        e.printStackTrace();
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
          registerSchema(wfid, wtid, fields.keySet());
          ioTDB.insert(wfid, wtid, timestamp, fields.toMap());
        } catch (Exception e) {
          logger.error("Receiving msg failed.", e);
          continue;
        }
        // TODO parse the msg and send it to iotdb
      }
    }

    private void registerSchema(String wfid, String wtid, Set<String> keySet) {

      schema.putIfAbsent(wfid, new ConcurrentHashMap<>());
      schema.get(wfid).putIfAbsent(wtid, new HashSet<>());
      Set<String> setInSchema = schema.get(wfid).get(wtid);

      synchronized (setInSchema) {
        for (String key : keySet) {
          if (!setInSchema.contains(key)) {
            for (int i = 0; i < TRY_NUM; i++) {
              //register
              boolean res = ioTDB.registerTimeSeries(wfid, wtid, key);
              //success
              if (res) {
                setInSchema.add(key);
                break;
              }
              // retry
              else {
                Session session = new Session(IOTDB_IP.getDefaultValue().toString(),
                    IOTDB_PORT.getDefaultValue().toString(),
                    IOTDB_USER.getDefaultValue().toString(),
                    IOTDB_PASSWARD.getDefaultValue().toString());
                try {
                  session.open();
                } catch (IoTDBSessionException e) {
                  logger.error("session create exception, {}", e);
                  e.printStackTrace();
                }
                ioTDB.setSession(session);
              }
            }
          } // if
        }// for
      }

    }
  }

  public static void main(String[] args) {
    // TODO read the properties, create a manager and run it
    Properties properties = loadProperties();
    ConsumerManager manager = new ConsumerManager(properties);
    manager.consume();

    long internalTimeIns = 5;
    new ScheduledThreadPoolExecutor(1)
        .scheduleAtFixedRate(manager.new LogThread(internalTimeIns), 0, internalTimeIns,
            TimeUnit.SECONDS);
  }

  class LogThread implements Runnable {

    long lastNum;
    long internalTime;

    public LogThread(long internalTimeIns) {
      this.internalTime = internalTimeIns;
      this.lastNum = 0;
    }

    @Override
    public void run() {
      long nowNum = insertPointNum.get();
      logger.warn(
          "Total timeseries number is {}, total point is {}, rate is {} points/s. Drop point is {}.",
          timeSeriesNum.get(), nowNum, (nowNum - lastNum) / internalTime, dropPointNum.get());
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
