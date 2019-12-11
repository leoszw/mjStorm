package com.manji.bolt.parse;

import com.manji.utils.*;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import sun.misc.Perf;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: app页面浏览事件解析 completed
 * User: szw
 * Date: 2019/11/22
 * Time: 15:57
 */
public class ParseAppViewScreenBolt implements IRichBolt {

    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    String timestamp = null;
    String hour = null;
    String days  = null;
    String week = null;
    String month = null;
    String quarter = null;
    String year = null;

    String appName = null;
    String event = null;
    String distinctId = null;
    String screenName;
    String version,title,userGroup,os;

    @Override
    public void execute(Tuple input) {
        HashMap hashMap = ParseLogUtil.parseLog(input.getString(0),input.getString(1));

        if(hashMap != null && null != hashMap.get("timestamp")){
            timestamp = HashMapUtil.getStrFromHashMap("timestamp",hashMap);
            hour = PerfixEnum.HOUR.getCode() + DateUtils.getHour(timestamp);
            days = PerfixEnum.DAY.getCode() + DateUtils.parseTime(timestamp);
            week = PerfixEnum.WEEK.getCode() + DateUtils.getWeek(timestamp);
            month = PerfixEnum.MONTH.getCode() + DateUtils.getMonth(timestamp);
            quarter = PerfixEnum.QUARTER.getCode() + DateUtils.getQuarter(timestamp);
            year = PerfixEnum.YEAR.getCode() + DateUtils.getYear(timestamp);

            appName = PerfixEnum.APP.getCode() + HashMapUtil.getStrFromHashMap("appName",hashMap);
            event = HashMapUtil.getStrFromHashMap("event",hashMap);
            distinctId = PerfixEnum.DISTINCTID.getCode() + HashMapUtil.getStrFromHashMap("distinctId",hashMap);
            screenName = HashMapUtil.getStrFromHashMap("screenName",hashMap);
            version = PerfixEnum.VERSION.getCode() + HashMapUtil.getStrFromHashMap("version",hashMap);
            title = HashMapUtil.getStrFromHashMap("title",hashMap);
            userGroup = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("userGroup", hashMap)) ?
                    null : PerfixEnum.USERGROUP.getCode() + HashMapUtil.getStrFromHashMap("userGroup", hashMap);
            os = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("os", hashMap)) ?
                    null : (PerfixEnum.OS.getCode() + HashMapUtil.getStrFromHashMap("os", hashMap));

            collector.emit(new Values(event,appName,year,quarter,month,week,days,hour,timestamp,distinctId,screenName,version,title,userGroup,os));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("event","appName","year","quarter","month","week","days","hour","timestamp","distinctId","screenName","version","title","userGroup","os"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
