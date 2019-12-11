package com.manji.bolt.parse;

import com.manji.utils.DateUtils;
import com.manji.utils.HashMapUtil;
import com.manji.utils.ParseLogUtil;
import com.manji.utils.PerfixEnum;
import org.apache.commons.lang.StringUtils;
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
 * Description: 页面浏览事件数据解析
 * User: szw
 * Date: 2019-09-27
 * Time: 15:01
 */
public class ParsePageViewLogBolt implements IRichBolt {
    OutputCollector collector = null;
    String urlPath;//当前路由
    String title;//页面名称
    String referrerPath;//前向路由
    String appName;//应用名称
    String event;//页面事件
    String referrerName;//前向名称（上页名称）
    String week;
    String month;
    String year;

    String timestamp = null;
    String hour = null;
    String days  = null;
    String quarter = null;


    String userAgent = null;
    String wapOrPc = null;
    String distinctId = null;

    static String[] agents = {"Android", "iPhone", "SymbianOS", "Windows Phone", "iPad", "iPod"};

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        HashMap hashMap = ParseLogUtil.parseLog(input.getString(0),input.getString(1));

        if (hashMap != null && hashMap.get("timestamp") != null) {
            //公共属性
            timestamp = HashMapUtil.getStrFromHashMap("timestamp",hashMap);     //数据时间Long型
            event = HashMapUtil.getStrFromHashMap("event",hashMap);     //事件名称
            hour = PerfixEnum.HOUR.getCode() + DateUtils.getHour(timestamp);
            days = PerfixEnum.DAY.getCode() + DateUtils.parseTime(timestamp);
            week = PerfixEnum.WEEK.getCode() + DateUtils.getWeek(timestamp);
            month = PerfixEnum.MONTH.getCode() + DateUtils.getMonth(timestamp);
            quarter = PerfixEnum.QUARTER.getCode() + DateUtils.getQuarter(timestamp);
            year = PerfixEnum.YEAR.getCode() + DateUtils.getYear(timestamp);

            appName = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("appName", hashMap)) ?
                    null : (PerfixEnum.APP.getCode() + HashMapUtil.getStrFromHashMap("appName", hashMap)); //应用名称

            urlPath = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("urlPath", hashMap)) ?
                    null : PerfixEnum.INDEXPAGE.getCode() + HashMapUtil.getStrFromHashMap("urlPath", hashMap);         //当前页面路径
            referrerPath = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("referrerPath", hashMap)) ?
                    null : PerfixEnum.SUPERPAGE.getCode() + HashMapUtil.getStrFromHashMap("referrerPath", hashMap);//上级页面路径

            title = HashMapUtil.getStrFromHashMap("title", hashMap);                 //当前页面标题
            referrerName = HashMapUtil.getStrFromHashMap("referrerName", hashMap);   //上级页面标题

            userAgent = HashMapUtil.getValFromHashMap("userAgent",hashMap,String.class);
            if(null != userAgent){
                for(String str : agents){
                    if(userAgent.indexOf(str) != -1){
                        wapOrPc = "WAP";
                        break;
                    }
                }
            }
            if(null == wapOrPc) wapOrPc = "PC";
            distinctId = PerfixEnum.DISTINCTID.getCode() + HashMapUtil.getStrFromHashMap("distinctId",hashMap);

            //发送数据
            collector.emit(new Values(hour,days,week,month,quarter,year,timestamp,urlPath, title, referrerPath, appName, event, referrerName,distinctId,wapOrPc));
        }else{
            try {
                throw new Exception("数据缺少time属性");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hour","days","week","month","quarter","year","timestamp","urlPath",
                " title","referrerPath","appName","event","referrerName","distinctId","wapOrPc"));
    }

    public void cleanup() {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
