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
 * Description: 保存pv数  completed
 * User: szw
 * Date: 2019/11/22
 * Time: 15:41
 */
public class SavePVBolt implements IRichBolt {
    String tableName;
    String CF = "cf_infor";
    String wapOrPc = null;
    String appName, year, quarter, month, week, days, hour, timestamp;

    public SavePVBolt(String tableName) {
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
        //"event","appName","year","quarter","month","week","days","hour"
        //app
        if ("$AppViewScreen".equals(input.getStringByField("event"))) {
            try {
                HbaseUtil.addRow(tableName, appName + "TERMINAL:APP", CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + "TERMINAL:APP", CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + quarter + "TERMINAL:APP", CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + month + "TERMINAL:APP", CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + week + "TERMINAL:APP", CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + "TERMINAL:APP", CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + hour + "TERMINAL:APP", CF, "dataTime", timestamp);

                HbaseUtil.incrementColumnValue(tableName, appName + "TERMINAL:APP", CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + "TERMINAL:APP", CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + "TERMINAL:APP", CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + "TERMINAL:APP", CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + "TERMINAL:APP", CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + "TERMINAL:APP", CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + "TERMINAL:APP", CF, "viewCount", 1L);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //wap or pc
        else if ("$pageview".equals(input.getStringByField("event"))) {
            wapOrPc = input.getStringByField("wapOrPc");
            try {
                HbaseUtil.addRow(tableName, appName + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + quarter + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + month + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + week + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + hour + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp);

                HbaseUtil.incrementColumnValue(tableName, appName + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // all (app & wap & pc)
        try {
            HbaseUtil.addRow(tableName, appName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour, CF, "dataTime", timestamp);

            HbaseUtil.incrementColumnValue(tableName, appName, CF, "viewCount", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year, CF, "viewCount", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter, CF, "viewCount", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month, CF, "viewCount", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week, CF, "viewCount", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days, CF, "viewCount", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour, CF, "viewCount", 1L);
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
