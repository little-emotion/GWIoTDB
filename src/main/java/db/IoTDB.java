package db;

import static consumer.ConsumerProperties.IOTDB_IP;
import static consumer.ConsumerProperties.IOTDB_PASSWARD;
import static consumer.ConsumerProperties.IOTDB_PORT;
import static consumer.ConsumerProperties.IOTDB_USER;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.service.rpc.thrift.TSStatusType;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
  private static final String GROUP_PREFIX = "root.g_";
  private static final Pattern PATTERN = Pattern.compile("[-]{0,1}[0-9]+[.]{0,1}[0-9]*[dD]{0,1}");
  private final static int TRY_NUM = 3;
  private int groupNum;
  private Session session;
  private ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, TSDataType>>> schema;

  private AtomicLong timeSeriesNum;
  private AtomicLong pointNum;
  private AtomicLong dropPointNum;

  public IoTDB() {
    try {
      session = new Session(IOTDB_IP.getDefaultValue().toString(),
          IOTDB_PORT.getDefaultValue().toString(), IOTDB_USER.getDefaultValue().toString(),
          IOTDB_PASSWARD.getDefaultValue().toString());
      session.open();
    } catch (IoTDBSessionException e) {
      LOGGER.error("register storage group error, because {}", e);
    }
  }

  public IoTDB(Session session,
      ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, TSDataType>>> schema,
      AtomicLong timeSeriesNum, AtomicLong pointNum,
      AtomicLong dropPointNum) {
    this.session = session;
    this.timeSeriesNum = timeSeriesNum;
    this.pointNum = pointNum;
    this.dropPointNum = dropPointNum;
    this.schema = schema;
  }

  public void registerStorageGroup(int num) throws IoTDBSessionException {
    this.groupNum = num;
    for (int i = 0; i < num; i++) {
      try {
        session.setStorageGroup(GROUP_PREFIX + i);
      } catch (IoTDBSessionException e) {
        LOGGER.error("An exception occurred when registering a storage group {}, because {}",
            GROUP_PREFIX + i, e);
        e.printStackTrace();
        throw e;
      }
    }
  }

  public void setGroupNum(int groupNum) {
    this.groupNum = groupNum;
  }

  /**
   * @return true-if succeed, false-if fail.
   */
  public boolean registerTimeSeries(String wfid, String wtid, String metric, boolean isNumType) {
    StringBuilder path = new StringBuilder(genPathPrefix(wfid, wtid)).append(".");
    LOGGER
        .debug("Register timeseries path = {}, isDouble = {}", path.toString() + metric, isNumType);
    TSStatus resp;
    try {
      if (isNumType) {
        resp = session
            .createTimeseries(path.toString() + metric+"_NUM", TSDataType.DOUBLE, TSEncoding.GORILLA,
                CompressionType.SNAPPY);
      } else {
        resp = session.createTimeseries(path.toString() + metric+"_STR", TSDataType.TEXT, TSEncoding.PLAIN,
            CompressionType.SNAPPY);
      }
      if (resp.statusType.getCode() ==  TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        timeSeriesNum.incrementAndGet();
        return true;
      } else {
        LOGGER
            .error("Register error, path = {}, isDouble = {}", path.toString() + metric, isNumType);
        return false;
      }
    } catch (IoTDBSessionException e) {
      LOGGER.error("An exception occurred when registering a timeseries {}, because {}",
          path.toString() + metric, e);
      return false;
    }
  }

  public void insert(String wfid, String wtid, long time, Map<String, Object> metricMap) {
    if (metricMap.isEmpty()) {
      return;
    }
    schema.putIfAbsent(wfid, new ConcurrentHashMap<>());
    schema.get(wfid).putIfAbsent(wtid, new ConcurrentHashMap<>());
    ConcurrentHashMap<String, TSDataType> metricTypeMap = schema.get(wfid).get(wtid);

    String deviceId = genPathPrefix(wfid, wtid);
    Iterator<Entry<String, Object>> iterable = metricMap.entrySet().iterator();
    while (iterable.hasNext()) {
      Entry<String, Object> entry = iterable.next();
      String metric = entry.getKey();
      // include
      if (metricTypeMap.containsKey(metric)) {
        // text
        if (metricTypeMap.get(metric).equals(TSDataType.TEXT)) {
          metricMap.put(metric, "'" + metricMap.get(metric) + "'");
        }
        else {
          if(isStringType(metric, entry.getValue().toString())){
            iterable.remove();
          }
        }
        continue;
      }
      // not include
      if (isStringType(metric, entry.getValue().toString())) {
        registerSchema(wfid, wtid, metric, false);
        metricMap.put(metric, "'" + metricMap.get(metric) + "'");
      } else {
        registerSchema(wfid, wtid, metric, true);
      }
    }

    if (metricMap.isEmpty()) {
      return;
    }
    List<String> metricNameList = new ArrayList();
    List<String> metricValueList = new ArrayList(metricMap.values());

    for(String k : metricMap.keySet()){
      if(metricTypeMap.get(k).equals(TSDataType.DOUBLE)){
        metricNameList.add(k+"_NUM");
      }
      else {
        metricNameList.add(k+"_STR");
      }
    }
    try {
      TSStatus resp = session.insert(deviceId, time, metricNameList, metricValueList);
      if (resp.statusType.getCode() ==  TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        pointNum.getAndAdd(metricValueList.size());
      } else {
        dropPointNum.getAndAdd(metricValueList.size());
      }

    } catch (IoTDBSessionException e) {
      LOGGER.error("An exception occurred when insert, time = {}, device = {} because {}", time,
          deviceId, e);
    }
  }

  private boolean isStringType(String metric, String value) {
    if (metric.contains("_Dt_") || metric.endsWith("_Dt")) {
      return true;
    }
    if (metric.endsWith("_S") || metric.contains("_S_")) {
      return true;
    }
    Matcher isNum = PATTERN.matcher(value);
    return !isNum.matches();
  }

  private void registerSchema(String wfid, String wtid, String metric, boolean isNumType) {
    ConcurrentHashMap<String, TSDataType> metricTypeMap = schema.get(wfid).get(wtid);
    synchronized (metricTypeMap) {
      if (!metricTypeMap.containsKey(metric)) {
        for (int i = 0; i < TRY_NUM; i++) {
          //register
          boolean res = registerTimeSeries(wfid, wtid, metric, isNumType);
          //success
          if (res) {
            if (isNumType) {
              metricTypeMap.put(metric, TSDataType.DOUBLE);
            } else {
              metricTypeMap.put(metric, TSDataType.TEXT);
            }
            break;
          }
          // retry
          else {
            session = new Session(IOTDB_IP.getDefaultValue().toString(),
                IOTDB_PORT.getDefaultValue().toString(),
                IOTDB_USER.getDefaultValue().toString(),
                IOTDB_PASSWARD.getDefaultValue().toString());
            try {
              session.open();
            } catch (IoTDBSessionException e) {
              LOGGER.error("session create exception, {}", e);
              e.printStackTrace();
            }
          }
        }
      }// if

    }

  }

  public void closeSession() {
    try {
      session.close();
    } catch (IoTDBSessionException e) {
      e.printStackTrace();
      LOGGER.error("close session error, because {}", e);
    }
  }

  private String genPathPrefix(String wfid, String wtid) {
    StringBuilder path = new StringBuilder(GROUP_PREFIX);
    path.append(wfid.hashCode() % groupNum).append(".").append(wfid).append(".").append(wtid);
    return path.toString();
  }
  // count timeseries root;

  // count timeseries root group by level=3 ;


}
