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
 * Description: app升级事件解析
 * User: szw
 * Date: 2019-09-27
 * Time: 15:01
 */
public class ParseUpdateAppLogBolt implements IRichBolt {
    OutputCollector collector = null;
    String times;       //时间
    String version;     //app版本信息
    String appName;     //app名称
    String province;    //省份
    String userID;      //用户id
    String event;       //事件
    String userGroup;   //用户组
    String week;
    String month;
    String year;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        HashMap hashMap = ParseLogUtil.parseLog(input.getString(0),input.getString(1));

        if (hashMap != null && hashMap.get("times") != null) {
//            //公共属性
            times = HashMapUtil.getStrFromHashMap("times", hashMap);     //数据时间Long型
            event = HashMapUtil.getStrFromHashMap("event", hashMap);     //事件名称
            year = PerfixEnum.YEAR.getCode() + DateUtils.getYear(times);      //数据时间年YEAR
            month = PerfixEnum.MONTH.getCode() + DateUtils.getMonth(times);  //数据时间月MONTH
            week = PerfixEnum.WEEK.getCode() + DateUtils.getWeek(times);     //数据时间周WEEK
            times = PerfixEnum.DAY.getCode() + DateUtils.parseTime(times);    //数据时间DAY
            appName = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("appName", hashMap)) ?
                    null : (PerfixEnum.APP.getCode() + HashMapUtil.getStrFromHashMap("appName", hashMap)); //应用名称
            province = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("province", hashMap)) ?
                    null : PerfixEnum.PROVINCE.getCode() + HashMapUtil.getStrFromHashMap("province", hashMap); //省份
            version = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("version", hashMap)) ?
                    null : PerfixEnum.VERSION.getCode() + HashMapUtil.getStrFromHashMap("version", hashMap);//版本

            userID = (StringUtils.isBlank(HashMapUtil.getStrFromHashMap("userID", hashMap)) &&
                    StringUtils.isBlank(HashMapUtil.getStrFromHashMap("equipment", hashMap))) ?
                    null : PerfixEnum.USER.getCode() + (StringUtils.isBlank(HashMapUtil.getStrFromHashMap("userID", hashMap)) ?
                    HashMapUtil.getStrFromHashMap("equipment", hashMap) : HashMapUtil.getStrFromHashMap("userID", hashMap));  //用id(不存在用户id使用设备号)
            userGroup = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("userGroup", hashMap)) ?
                    null : PerfixEnum.USERGROUP.getCode() + HashMapUtil.getStrFromHashMap("userGroup", hashMap);

            //发送数据
            collector.emit(new Values(times, week, month, year, appName, event, version, province, userID, userGroup));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("times", "week", "month", "year", "appName", "event", "version", "province", "userID", "userGroup"));
    }

    public void cleanup() {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
