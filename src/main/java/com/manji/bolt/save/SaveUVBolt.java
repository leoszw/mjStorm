package com.manji.bolt.save;

import com.manji.utils.HbaseUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 保存uv数 （记录的是明细，统计需去重） completed
 * User: szw
 * Date: 2019/11/22
 * Time: 15:41
 */
public class SaveUVBolt implements IRichBolt {
    String tableName;
    String CF = "cf_infor";
    String wapOrPc = null;
    String appName, year, quarter, month, week, days, hour, timestamp, distinctId;

    public SaveUVBolt(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        appName = input.getStringByField("appName");
        year = input.getStringByField("year");
        quarter = input.getStringByField("quarter");
        month = input.getStringByField("month");
        week = input.getStringByField("week");
        days = input.getStringByField("days");
        hour = input.getStringByField("hour");
        timestamp = input.getStringByField("timestamp");
        distinctId = input.getStringByField("distinctId");
        //"event","appName","year","quarter","month","week","days","hour"
        //app
        if ("$AppViewScreen".equals(input.getStringByField("event"))) {
            try {
                HbaseUtil.addRow(tableName, appName + "TERMINAL:APP" + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + "TERMINAL:APP" + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + quarter + "TERMINAL:APP" + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + month + "TERMINAL:APP" + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + week + "TERMINAL:APP" + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + "TERMINAL:APP" + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + hour + "TERMINAL:APP" + distinctId, CF, "dataTime", timestamp);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //wap or pc
        else if ("$pageview".equals(input.getStringByField("event"))) {
            wapOrPc = input.getStringByField("wapOrPc");
            try {
                HbaseUtil.addRow(tableName, appName + "TERMINAL:" + wapOrPc + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + "TERMINAL:" + wapOrPc + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + quarter + "TERMINAL:" + wapOrPc + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + month + "TERMINAL:" + wapOrPc + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + week + "TERMINAL:" + wapOrPc + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + "TERMINAL:" + wapOrPc + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + hour + "TERMINAL:" + wapOrPc + distinctId, CF, "dataTime", timestamp);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // all (app & wap & pc)
        try {
            HbaseUtil.addRow(tableName, appName + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + distinctId, CF, "dataTime", timestamp);
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
