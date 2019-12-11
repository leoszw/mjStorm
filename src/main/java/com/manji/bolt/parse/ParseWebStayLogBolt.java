package com.manji.bolt.parse;

import com.manji.utils.DateUtils;
import com.manji.utils.HashMapUtil;
import com.manji.utils.ParseLogUtil;
import com.manji.utils.PerfixEnum;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 页面停留事件数据解析
 * User: szw
 * Date: 2019-09-27
 * Time: 15:01
 */
public class ParseWebStayLogBolt implements IRichBolt {
    OutputCollector collector = null;
    String timestamp;//时间
    String urlPath;//当前路由
    String appName;//应用名称
    String event;//页面事件
    Long useTime;//页面停留时间
    String version;//版本
    String userGroup;//用户组
    String week;
    String month;
    String year;
    String days;
    String hour;
    String quarter;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        HashMap hashMap = ParseLogUtil.parseLog(input.getString(0),input.getString(1));

        if (hashMap != null && hashMap.get("times") != null) {
            //公共 属性
            timestamp = HashMapUtil.getStrFromHashMap("timestamp",hashMap);     //数据时间Long型
            event = HashMapUtil.getStrFromHashMap("event",hashMap);     //事件名称

            year = PerfixEnum.YEAR.getCode() + DateUtils.getYear(timestamp);      //数据时间年YEAR
            month = PerfixEnum.MONTH.getCode() + DateUtils.getMonth(timestamp);  //数据时间月MONTH
            week = PerfixEnum.WEEK.getCode() + DateUtils.getWeek(timestamp);     //数据时间周WEEK
            days = PerfixEnum.DAY.getCode() + DateUtils.parseTime(timestamp);    //数据时间DAY
            hour = PerfixEnum.HOUR.getCode() + DateUtils.getHour(timestamp);
            quarter = PerfixEnum.QUARTER.getCode() + DateUtils.getQuarter(timestamp);

            appName = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("appName", hashMap)) ?
                    null : PerfixEnum.APP.getCode() + HashMapUtil.getStrFromHashMap("appName", hashMap); //应用名称
            urlPath = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("urlPath", hashMap)) ?
                    null : PerfixEnum.INDEXPAGE.getCode() + HashMapUtil.getStrFromHashMap("urlPath", hashMap); //停留页面路径
            useTime = Math.round(HashMapUtil.getDoubleFromHashMap("pageStay", hashMap)*1000); //页面停留时间

            version = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("version", hashMap)) ?
                    null : PerfixEnum.VERSION.getCode() + HashMapUtil.getStrFromHashMap("version", hashMap);
            userGroup = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("userGroup", hashMap)) ?
                    null : PerfixEnum.USERGROUP.getCode() + HashMapUtil.getStrFromHashMap("userGroup", hashMap);

            //发送数据
            collector.emit(new Values(timestamp,hour,days, week, month,quarter, year,urlPath, appName, event, useTime, version, userGroup));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp","hour","days","week","month","quarter","year","urlPath","appName","event","useTime","version","userGroup"));
    }

    public void cleanup() {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
