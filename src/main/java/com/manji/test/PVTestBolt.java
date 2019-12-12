package com.manji.test;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 保存pv数  completed
 * User: szw
 * Date: 2019/11/22
 * Time: 15:41
 */
public class PVTestBolt implements IRichBolt {
    String CF = "cf_infor";
    String wapOrPc = null;
    String appName, year, quarter, month, week, days, hour, timestamp;
    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
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

        if ("$AppViewScreen".equals(input.getStringByField("event"))) {
            try {
                collector.emit(new Values(appName + "TERMINAL:APP", CF, "dataTime", timestamp, false));
                collector.emit(new Values(appName + year + "TERMINAL:APP", CF, "dataTime", timestamp, false));
                collector.emit(new Values(appName + year + quarter + "TERMINAL:APP", CF, "dataTime", timestamp, false));
                collector.emit(new Values(appName + year + month + "TERMINAL:APP", CF, "dataTime", timestamp, false));
                collector.emit(new Values(appName + year + week + "TERMINAL:APP", CF, "dataTime", timestamp, false));
                collector.emit(new Values(appName + days + "TERMINAL:APP", CF, "dataTime", timestamp, false));
                collector.emit(new Values(appName + days + hour + "TERMINAL:APP", CF, "dataTime", timestamp, false));

                collector.emit(new Values(appName + "TERMINAL:APP", CF, "viewCount", 1L, true));
                collector.emit(new Values(appName + year + "TERMINAL:APP", CF, "viewCount", 1L, true));
                collector.emit(new Values(appName + year + quarter + "TERMINAL:APP", CF, "viewCount", 1L, true));
                collector.emit(new Values(appName + year + month + "TERMINAL:APP", CF, "viewCount", 1L, true));
                collector.emit(new Values(appName + year + week + "TERMINAL:APP", CF, "viewCount", 1L, true));
                collector.emit(new Values(appName + days + "TERMINAL:APP", CF, "viewCount", 1L, true));
                collector.emit(new Values(appName + days + hour + "TERMINAL:APP", CF, "viewCount", 1L, true));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //wap or pc
        else if ("$pageview".equals(input.getStringByField("event"))) {
            wapOrPc = input.getStringByField("wapOrPc");
            try {
                collector.emit(new Values(appName + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp, false));
                collector.emit(new Values(appName + year + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp, false));
                collector.emit(new Values(appName + year + quarter + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp, false));
                collector.emit(new Values(appName + year + month + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp, false));
                collector.emit(new Values(appName + year + week + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp, false));
                collector.emit(new Values(appName + days + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp, false));
                collector.emit(new Values(appName + days + hour + "TERMINAL:" + wapOrPc, CF, "dataTime", timestamp, false));

                collector.emit(new Values(appName + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L, true));
                collector.emit(new Values(appName + year + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L, true));
                collector.emit(new Values(appName + year + quarter + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L, true));
                collector.emit(new Values(appName + year + month + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L, true));
                collector.emit(new Values(appName + year + week + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L, true));
                collector.emit(new Values(appName + days + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L, true));
                collector.emit(new Values(appName + days + hour + "TERMINAL:" + wapOrPc, CF, "viewCount", 1L, true));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // all (app & wap & pc)
        try {
            collector.emit(new Values(appName, CF, "dataTime", timestamp, false));
            collector.emit(new Values(appName + year, CF, "dataTime", timestamp, false));
            collector.emit(new Values(appName + year + quarter, CF, "dataTime", timestamp, false));
            collector.emit(new Values(appName + year + month, CF, "dataTime", timestamp, false));
            collector.emit(new Values(appName + year + week, CF, "dataTime", timestamp, false));
            collector.emit(new Values(appName + days, CF, "dataTime", timestamp, false));
            collector.emit(new Values(appName + days + hour, CF, "dataTime", timestamp, false));

            collector.emit(new Values(appName, CF, "viewCount", 1L, true));
            collector.emit(new Values(appName + year, CF, "viewCount", 1L, true));
            collector.emit(new Values(appName + year + quarter, CF, "viewCount", 1L, true));
            collector.emit(new Values(appName + year + month, CF, "viewCount", 1L, true));
            collector.emit(new Values(appName + year + week, CF, "viewCount", 1L, true));
            collector.emit(new Values(appName + days, CF, "viewCount", 1L, true));
            collector.emit(new Values(appName + days + hour, CF, "viewCount", 1L, true));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rowKey", "cf", "cq", "value", "isNum"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
