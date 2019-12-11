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
 * Description: app结束事件解析
 * User: szw
 * Date: 2019-09-27
 * Time: 15:01
 */
public class ParseAppEndLogBolt implements IRichBolt {
    OutputCollector collector = null;
    String timestamp;//时间
    String version;//app版本信息
    String appName;//app名称
    String province;//省份
    String userID;//用户id
    String event;//事件
    String userGroup;//用户群
    Long useTime;//使用时长
    String brand;       //品牌
    String model;       //设备型号
    String networkType; //联网方式
    String os;          //操作系统
    String resolutionRatio;//分辨率
    String week;
    String month;
    String year;
    String quarter;
    String days;
    String hour;
    String distinctId;
    String screenName,title;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        HashMap hashMap = ParseLogUtil.parseLog(input.getString(0),input.getString(1));

        if (hashMap != null && hashMap.get("timestamp") != null) {
            //公共属性
            timestamp = HashMapUtil.getStrFromHashMap("timestamp",hashMap);     //数据时间Long型
            event = HashMapUtil.getStrFromHashMap("event",hashMap);     //事件名称
            year = PerfixEnum.YEAR.getCode() + DateUtils.getYear(timestamp);      //数据时间年YEAR
            quarter = PerfixEnum.QUARTER.getCode() + DateUtils.getQuarter(timestamp);
            month = PerfixEnum.MONTH.getCode() + DateUtils.getMonth(timestamp);   //数据时间月MONTH
            week = PerfixEnum.WEEK.getCode() + DateUtils.getWeek(timestamp);      //数据时间周WEEK
            days = PerfixEnum.DAY.getCode() + DateUtils.parseTime(timestamp);        //数据时间DAY
            hour = PerfixEnum.HOUR.getCode() + DateUtils.getHour(timestamp);
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

            useTime = Math.round(HashMapUtil.getDoubleFromHashMap("useTime", hashMap) * 1000);

            brand = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("brand", hashMap)) ?
                    null : (PerfixEnum.BRAND.getCode() + HashMapUtil.getStrFromHashMap("brand", hashMap));
            model = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("model", hashMap)) ?
                    null : (PerfixEnum.MODEL.getCode() + HashMapUtil.getStrFromHashMap("model", hashMap));
            networkType = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("networkType", hashMap)) ?
                    null : (PerfixEnum.NETWORKTYPE.getCode() + HashMapUtil.getStrFromHashMap("networkType", hashMap));
            os = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("os", hashMap)) ?
                    null : (PerfixEnum.OS.getCode() + HashMapUtil.getStrFromHashMap("os", hashMap));
            resolutionRatio = StringUtils.isBlank(HashMapUtil.getStrFromHashMap("resolutionRatio", hashMap)) ?
                    null : (PerfixEnum.RESOLUTIONRATIO.getCode() + HashMapUtil.getStrFromHashMap("resolutionRatio", hashMap));
            distinctId = PerfixEnum.DISTINCTID.getCode()+ HashMapUtil.getStrFromHashMap("distinctId",hashMap);
            screenName = HashMapUtil.getStrFromHashMap("screenName",hashMap);
            title = HashMapUtil.getStrFromHashMap("title",hashMap);

            //发送数据
            collector.emit(new Values(timestamp,hour,days, week, month,quarter, year, appName, event, version, province, userID, userGroup,
                    useTime, brand, model, networkType, os, resolutionRatio,distinctId,screenName,title));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp","hour","days", "week", "month", "quarter","year", "appName", "event", "version", "province",
                "userID", "userGroup", "useTime", "brand", "model", "networkType", "os", "resolutionRatio","distinctId","screenName","title"));
    }

    public void cleanup() {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
