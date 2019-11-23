package consumer;


import static consumer.ConsumerProperties.CONSUMER_THREAD_NUM;
import static consumer.ConsumerProperties.IOTDB_GROUP_NUM;
import static consumer.ConsumerProperties.IOTDB_IP;
import static consumer.ConsumerProperties.IOTDB_PASSWARD;
import static consumer.ConsumerProperties.IOTDB_PORT;
import static consumer.ConsumerProperties.IOTDB_USER;

import com.csvreader.CsvReader;
import db.IoTDB;
import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerManager {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerManager.class);

  private ExecutorService consumerPool;

  private int threadNum;
  private int groupNum;

  private AtomicLong insertPointNum = new AtomicLong();
  private AtomicLong dropPointNum = new AtomicLong();

  public static char separator = ',';
  public final static String DIR = "/data6/csv";

  private ConsumerManager(Properties properties) {
    threadNum = Integer.parseInt(properties
        .getProperty(CONSUMER_THREAD_NUM.getPropertyName(),
            CONSUMER_THREAD_NUM.getDefaultValue().toString()));
  }

  private void consume() throws IoTDBSessionException {

    groupNum = Integer.parseInt(IOTDB_GROUP_NUM.getDefaultValue().toString());
    IoTDB ioTDB = new IoTDB();
    ioTDB.registerStorageGroup(groupNum);
    ioTDB.closeSession();

    List<String> filePath = getAllCSVFile(DIR);
    filePath.sort(new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        //real_640401057_20191013.csv
        o1 = o1.replace(".csv","");
        o2 = o2.replace(".csv","");

        String[] o1Arr = o1.split("_");
        String[] o2Arr = o2.split("_");

        int wfid1 = Integer.parseInt(o1Arr[1]);
        int wfid2 = Integer.parseInt(o2Arr[1]);
        if(wfid1 != wfid2){
          return wfid1 - wfid2;
        }

        int date1 = Integer.parseInt(o1Arr[2]);
        int date2 = Integer.parseInt(o2Arr[2]);

        return date1 - date2;
      }
    });
    List<List<String>> threadPath = averageAssign(filePath);

    logger.info("total file num is {}, total device is {}.", filePath.size(), threadPath.size());
    consumerPool = Executors.newFixedThreadPool(threadPath.size());
    for (int i = 0; i<threadPath.size();i++) {
      consumerPool.submit(new ConsumeTask(threadPath.get(i), insertPointNum, dropPointNum));
      //consumerPool.submit(new ConsumeTask(filePath, insertPointNum, dropPointNum));
    }
  }



  class ConsumeTask implements Runnable {

    private List<String> fileList;
    private IoTDB ioTDB;
    private int curIndex;
    private CsvReader reader = null;

    private ConsumeTask(List<String> fileList, AtomicLong pointNum, AtomicLong dropPointNum) {
      this.fileList = fileList;
      curIndex = 0;
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
      logger.info("{}" + Thread.currentThread().getName() + " run!" + fileList.toString());
      while (curIndex < fileList.size()) {
        try {
          //如果生产文件乱码，windows下用gbk，linux用UTF-8
          reader = new CsvReader(fileList.get(curIndex), separator, Charset.forName("UTF-8"));
          // 读取表头
          reader.readHeaders();
          String[] headArray = reader.getHeaders();//获取标题
          List<String> headList = new ArrayList<>();
          for (int i = 3; i < headArray.length; i++) {
            headList.add(headArray[i].replace('.','_') + "_NUM");
          }
          // 逐条读取记录，直至读完
          while (reader.readRecord()) {
            String[] valueArray = reader.getValues();
            ioTDB.insert(headList, valueArray);
          }
          logger.info("Thread {}, File {} completed. Progress {}/{}.",
              Thread.currentThread().getName(), fileList.get(curIndex), curIndex+1, fileList.size());
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          curIndex++;

          if (null != reader) {
            reader.close();
          }
        }
      }
      logger.info("Thread {} ended.", Thread.currentThread().getName());
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

    long internalTimeIns = 60;
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
    return properties;
  }

  private static List<String>  getAllCSVFile(String dir) {
    List<String> fileNameList = new ArrayList<>();
    //Traverse to find all tsfiles
    File file = new File(dir);
    Queue<File> tmp = new LinkedList<>();
    tmp.add(file);

    if (file.exists()) {
      while (!tmp.isEmpty()) {
        File tmp_file = tmp.poll();
        File[] files = tmp_file.listFiles();
        for (File file2 : files) {
          if (file2.isDirectory()) {
            tmp.add(file2);
          } else {
            if (file2.getName().endsWith(".csv") && file2.getName().startsWith("real")) {
              fileNameList.add(file2.getAbsolutePath());
            }
          }
        }
      }
    }
    return fileNameList;
  }

  public static List<List<String>> averageAssign(List<String> source) {


    Map<Integer, List<String>> mp = new HashMap<>();
    for(String str : source){
      //real_640401057_20191013.csv
      String o1 = str.replace(".csv","");
      String[] o1Arr = o1.split("_");
      int wfid1 = Integer.parseInt(o1Arr[1]);
      //int date1 = Integer.parseInt(o1Arr[2]);
      mp.putIfAbsent(wfid1, new ArrayList<>());
      mp.get(wfid1).add(str);
    }
    List<List<String>> result = new ArrayList<>();
    for(List<String> fileList : mp.values()){
      result.add(fileList);
    }
    return result;
  }

  public static <T> List<List<T>> averageAssign(List<T> source, int n) {
    List<List<T>> result = new ArrayList<>();
    //(先计算出余数)
    int remainder = source.size() % n;
    //然后是商
    int number = source.size() / n;
    //偏移量
    int offset = 0;
    for (int i = 0; i < n; i++) {
      List<T> value;
      if (remainder > 0) {
        value = source.subList(i * number + offset, (i + 1) * number + offset + 1);
        remainder--;
        offset++;
      } else {
        value = source.subList(i * number + offset, (i + 1) * number + offset);
      }
      result.add(value);
    }
    return result;
  }


}
