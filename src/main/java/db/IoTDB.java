package db;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
  private int groupNum;
  private Session session;

  private AtomicLong timeSeriesNum;
  private AtomicLong pointNum;
  private AtomicLong dropPointNum;


  public IoTDB(Session session, AtomicLong timeSeriesNum, AtomicLong pointNum,
      AtomicLong dropPointNum) {
    this.session = session;
    this.timeSeriesNum = timeSeriesNum;
    this.pointNum = pointNum;
    this.dropPointNum = dropPointNum;
  }

  public void registerStorageGroup(int num) throws IoTDBSessionException {
    this.groupNum = num;
    for (int i = 0; i < num; i++) {
      try {
        session.setStorageGroup(GROUP_PREFIX + i);
      } catch (IoTDBSessionException e) {
        LOGGER.error("An exception occurred when registering a storage group {}, because {}",
            GROUP_PREFIX + i, e);
        throw e;
      }
    }
  }

  public void registerTimeSeries(String wfid, String wtid, Set<String> metricSet) {
    for (String metric : metricSet) {
      registerTimeSeries(wfid, wtid, metric);
    }
  }

  /**
   * @return true-if succeed, false-if fail.
   */
  public boolean registerTimeSeries(String wfid, String wtid, String metric) {
    StringBuilder path = new StringBuilder(genPathPrefix(wfid, wtid)).append(".");
    try {
      session.createTimeseries(path.toString() + metric, TSDataType.DOUBLE, TSEncoding.GORILLA,
          CompressionType.SNAPPY);
      timeSeriesNum.incrementAndGet();
      return true;
    } catch (IoTDBSessionException e) {
      LOGGER.error("An exception occurred when registering a timeseries {}, because {}",
          path.toString() + metric, e);
      return false;
    }
  }

  public void insert(String wfid, String wtid, long time, Map<String, Object> metricMap) {
    String deviceId = genPathPrefix(wfid, wtid);
    Iterator<Entry<String, Object>> iterable = metricMap.entrySet().iterator();
    while (iterable.hasNext()) {
      Entry<String, Object> entry = iterable.next();
      Matcher isNum = PATTERN.matcher(entry.getValue().toString());
      if (!isNum.matches()) {
        LOGGER.info("[DROP]: time = {}, device = {}, metric = {}, value = {}.", time,
            deviceId, entry.getKey(), entry.getValue());
        iterable.remove();
        dropPointNum.incrementAndGet();
      }
    }

    List<String> metricNameList = new ArrayList(metricMap.keySet());
    List<String> metricValueList = new ArrayList(metricMap.values());

    try {
      session.insert(deviceId, time, metricNameList, metricValueList);
      pointNum.getAndAdd(metricValueList.size());
    } catch (IoTDBSessionException e) {
      LOGGER.error("An exception occurred when insert, time = {}, device = {} because {}", time,
          deviceId, e);
    }
  }

  private String genPathPrefix(String wfid, String wtid) {
    StringBuilder path = new StringBuilder(GROUP_PREFIX);
    path.append(wfid.hashCode() % groupNum).append(".").append(wfid).append(".").append(wtid);
    return path.toString();
  }

  public void setSession(Session session) {
    this.session = session;
  }

  // count timeseries root;

  // count timeseries root groupby index by ;

}
