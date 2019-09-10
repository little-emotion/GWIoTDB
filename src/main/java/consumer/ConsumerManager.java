package consumer;

import static consumer.ConsumerProperties.CONSUMER_THREAD_NUM;
import static consumer.ConsumerProperties.TOPIC;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerManager {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerManager.class);

  private Properties properties;
  private ConsumerConnector consumerConnector;
  private ExecutorService consumerPool;

  private int threadNum;
  private String topic;

  public ConsumerManager(Properties properties) {
    this.properties = properties;

    properties.put("zookeeper.session.timeout.ms", "4000");
    properties.put("zookeeper.sync.time.ms", "200");
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
      consumerPool.submit(new ConsumeTask(stream));
    }
  }



  class ConsumeTask implements Runnable {

    private KafkaStream<String, String> stream;

    private ConsumeTask(KafkaStream<String, String> stream) {
      this.stream = stream;
      initDBConnection();
    }

    private void initDBConnection() {
      // TODO: establish a connection to IoTDB
    }

    public void run() {
      //get message
      for (MessageAndMetadata<String, String> message : stream) {
        String msg;
        try {
          msg = message.message();
        } catch (Exception e) {
          logger.error("Receiving msg failed.", e);
          continue;
        }
       // TODO parse the msg and send it to iotdb
      }
    }
  }

  public static void main(String[] args) {
    // TODO read the properties, create a manager and run it
  }
}
