package com.manji.bolt.save;

import com.manji.utils.DateUtils;
import com.manji.utils.HbaseUtil;
import com.manji.utils.PerfixEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 保存访客信息（在线访客和最近访客） completed
 * User: szw
 * Date: 2019/11/29
 * Time: 15:36
 */
public class SaveVisitorInfoBolt implements IRichBolt {
    String tableName;
    String CF = "cf_infor";

    String appName, distinctId, timestamp, version, province, os, model, resolutionRatio, networkType, screenName, event, userGroup;
    Long useTime;

    public SaveVisitorInfoBolt(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        event = input.getStringByField("event");
        appName = input.getStringByField("appName");
        distinctId = input.getStringByField("distinctId");
        timestamp = input.getStringByField("timestamp");
        version = input.getStringByField("version");
        os = input.getStringByField("os");
        screenName = input.getStringByField("screenName");
        // todo AppViewScreen 的 useTime 目前没有
        if ("$AppEnd".equals(event)) {
            useTime = input.getLongByField("useTime");
        }



        try {
            /** 用户最近登录信息(在线访客统计信息) **/
            if ("$AppStart".equals(event)) {
                province = input.getStringByField("province");
                networkType = input.getStringByField("networkType");
                model = input.getStringByField("model");
                resolutionRatio = input.getStringByField("resolutionRatio");

                HbaseUtil.addRow(tableName, appName + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + distinctId, CF, "version", version.replaceAll(PerfixEnum.VERSION.getCode(), ""));
                if(StringUtils.isNotBlank(province))
                HbaseUtil.addRow(tableName, appName + distinctId, CF, "province", province.replaceAll(PerfixEnum.PROVINCE.getCode(), ""));
                HbaseUtil.addRow(tableName, appName + distinctId, CF, "os", os.replaceAll(PerfixEnum.OS.getCode(), ""));
                HbaseUtil.addRow(tableName, appName + distinctId, CF, "model", model.replaceAll(PerfixEnum.MODEL.getCode(), ""));
                HbaseUtil.addRow(tableName, appName + distinctId, CF, "resolutionRatio", resolutionRatio.replaceAll(PerfixEnum.RESOLUTIONRATIO.getCode(), ""));
                HbaseUtil.addRow(tableName, appName + distinctId, CF, "networkType", networkType.replaceAll(PerfixEnum.NETWORKTYPE.getCode(), ""));
                //TODO 渠道？
            }

            /** 用户最近登录信息(近期访客统计信息) **/
            if ("$AppStart".equals(event)) {
                HbaseUtil.addRow(tableName, appName + "EVENT:S" + distinctId + timestamp, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + "EVENT:S" + distinctId + timestamp, CF, "version", version);
                HbaseUtil.addRow(tableName, appName + "EVENT:S" + distinctId + timestamp, CF, "province", province);
                HbaseUtil.addRow(tableName, appName + "EVENT:S" + distinctId + timestamp, CF, "screenName", screenName);
                // TODO 渠道？


                //版本和用户分组（使用行为统计时使用）
                HbaseUtil.addRow(tableName, appName + "EVENT:S" + version +distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + "EVENT:S" + userGroup + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + "EVENT:S" + version + userGroup + distinctId, CF, "dataTime", timestamp);
            } else if ("$AppViewScreen".equals(event)) {
                HbaseUtil.addRow(tableName, appName + "EVENT:V" + distinctId + timestamp, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + "EVENT:V" + distinctId + timestamp, CF, "screenName", screenName);
                //app页面浏览时间统计 类似于 webStay事件 统计页面停留时间，目前还没有
                //HbaseUtil.addRow(tableName, appName + "EVENT:V"+distinctId + timestamp,CF,"useTime",useTime);

                //版本和用户分组（使用行为统计时使用）
                HbaseUtil.addRow(tableName, appName + "EVENT:V" + version +distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + "EVENT:V" + userGroup + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + "EVENT:V" + version + userGroup + distinctId, CF, "dataTime", timestamp);
            } else if ("$AppEnd".equals(event)) {
                HbaseUtil.addRow(tableName, appName + "EVENT:E" + distinctId + timestamp, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + "EVENT:E" + distinctId + timestamp, CF, "screenName", screenName);
                HbaseUtil.addRow(tableName, appName + "EVENT:E" + distinctId + timestamp, CF, "useTime", useTime);

                //版本和用户分组（使用行为统计时使用）
                HbaseUtil.addRow(tableName, appName + "EVENT:V" + version +distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + "EVENT:V" + userGroup + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + "EVENT:V" + version + userGroup + distinctId, CF, "dataTime", timestamp);

                HbaseUtil.addRow(tableName, appName + "EVENT:V" + version +distinctId, CF, "useTime", useTime);
                HbaseUtil.addRow(tableName, appName + "EVENT:V" + userGroup + distinctId, CF, "useTime", useTime);
                HbaseUtil.addRow(tableName, appName + "EVENT:V" + version + userGroup + distinctId, CF, "useTime", useTime);
            }

            //启动间隔查询
            if ("$AppStart".equals(event)) {
                Long before = HbaseUtil.getBeforStartTime(tableName, appName + "EVENT:S" + distinctId + "[\\s\\S]+", CF, "dataTime");
                Integer interval = -1;
                if (null != before) {
                    interval = DateUtils.dateDifference(DateUtils.parseTime(before), DateUtils.parseTime(timestamp));
                }
                //保存距上次启动的天数，-1表示首次启动，0 当日，其他 天数差
                HbaseUtil.addRow(tableName, appName + "EVENT:S" + distinctId + timestamp, CF, "interval", interval);

                HbaseUtil.addRow(tableName, appName + "EVENT:S" + version +distinctId, CF, "interval", interval);
                HbaseUtil.addRow(tableName, appName + "EVENT:S" + userGroup + distinctId, CF, "interval", interval);
                HbaseUtil.addRow(tableName, appName + "EVENT:S" + version + userGroup + distinctId, CF, "interval", interval);
            }

            /**
             * 查询：
             * rowkey 正则表达式 appName + "EVENT:S" + "[\\s\\S]+" 匹配查询最近访客
             *
             * 取得 distinctId 循环查询 appName + "EVENT:V" + distinctId +"[\\s\\S]+"  && dataTime > 第一步查询对应的dataTime ，取得页面浏览数据
             *
             * 取得 distinctId 循环查询 appName + "EVENT:E" + distinctId +"[\\s\\S]+"  && dataTime > 第一步查询对应的dataTime ，取得退出页面数据
             *
             * 访问页数 EVENT:V 的数据条数
             */
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
