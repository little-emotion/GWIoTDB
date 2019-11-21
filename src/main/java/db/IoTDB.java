package db;

import static consumer.ConsumerProperties.IOTDB_IP;
import static consumer.ConsumerProperties.IOTDB_PASSWARD;
import static consumer.ConsumerProperties.IOTDB_PORT;
import static consumer.ConsumerProperties.IOTDB_USER;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.Session;
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

  //2019-03-01 00:01:39
  private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
            GROUP_PREFIX + i, e.toString());
        throw e;
      }
    }
  }

  public void setGroupNum(int groupNum) {
    this.groupNum = groupNum;
  }


  public void insert(List<String> metricNameList, String[] valueArray) {
    if(valueArray.length < 3){
      return;
    }
    String wfid = valueArray[1];
    String wtid = valueArray[2];

    String deviceId = genPathPrefix(wfid, wtid);
    List<String> metricValueList = new ArrayList<>();

    for (int i = 3; i < valueArray.length; i++) {
      String value = valueArray[i];
      if(value.contains(".")){
        metricValueList.add(value);
      }
      else {
        metricValueList.add(value+".0");
      }
    }

    String timeStr = valueArray[0];
    long time = 0;
    try {
      time = df.parse(timeStr).getTime();
    } catch (ParseException e) {
      dropPointNum.getAndAdd(metricValueList.size());
      LOGGER.error("timestr {} can't be parsed.", timeStr);
      return;
    }

    try {
      TSStatus resp = session.insert(deviceId, time, metricNameList, metricValueList);
      if (resp.statusType.getCode() ==  TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        pointNum.getAndAdd(metricValueList.size());
      } else {
        dropPointNum.getAndAdd(metricValueList.size());
      }

    } catch (IoTDBSessionException e) {
      LOGGER.error("An exception occurred when insert, time = {}, device = {}, metric = {}, value = {} because {}", time,
          deviceId, metricNameList, metricValueList, e);
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
