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
  private static final Pattern PATTERN = Pattern.compile("[-]?[0-9]+[.]?[0-9]*[dD]?");
  private int groupNum;
  private Session session;

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

  public IoTDB(Session session, AtomicLong pointNum, AtomicLong dropPointNum) {
    this.session = session;
    this.pointNum = pointNum;
    this.dropPointNum = dropPointNum;
  }

  public void registerStorageGroup(int num) throws IoTDBSessionException {
    this.groupNum = num;
    for (int i = 0; i < num; i++) {
      try {
        session.setStorageGroup(GROUP_PREFIX + i);
      } catch (IoTDBSessionException e) {
        if(e.getMessage().contains("already exist")){
          return;
        }
        LOGGER.error("An exception occurred when registering a storage group {}, because {}",
            GROUP_PREFIX + i, e);
        throw e;
      }
    }
  }

  public void setGroupNum(int groupNum) {
    this.groupNum = groupNum;
  }



  public void insert(String wfid, String wtid, long time, Map<String, Object> metricMap) {
    if (metricMap.isEmpty()) {
      return;
    }

    String deviceId = genPathPrefix(wfid, wtid);
    List<String> metricNameList = new ArrayList<>();
    List<String> metricValueList = new ArrayList<>();
    Iterator<Entry<String, Object>> iterable = metricMap.entrySet().iterator();
    while (iterable.hasNext()) {
      Entry<String, Object> entry = iterable.next();
      String metric = entry.getKey();
      String value = entry.getValue().toString();
      if (isStringType(metric, value)) {
        metricNameList.add(metric+"_STR");
        metricValueList.add("'"+value+"'");
      }
      else {
        metricNameList.add(metric+"_NUM");
        if(value.contains(".")){
          metricValueList.add(value);
        }
        else {
          metricValueList.add(value+".0");
        }
      }
    }
    pointNum.getAndAdd(metricValueList.size());
//    try {
//      TSStatus resp = session.insert(deviceId, time, metricNameList, metricValueList);
//      if (resp.statusType.getCode() ==  TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
//        pointNum.getAndAdd(metricValueList.size());
//      } else {
//        dropPointNum.getAndAdd(metricValueList.size());
//      }
//
//    } catch (IoTDBSessionException e) {
//      LOGGER.error("An exception occurred when insert, time = {}, device = {} because {}", time,
//          deviceId, e);
//    }
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

  public void closeSession() {
    try {
      session.close();
    } catch (IoTDBSessionException e) {
      LOGGER.error("close session error, because:", e);
    }
  }

  private String genPathPrefix(String wfid, String wtid) {
    return GROUP_PREFIX + wfid.hashCode() % groupNum + "." + wfid + "." + wtid;
  }
  // count timeseries root;

  // count timeseries root group by level=3 ;


}
