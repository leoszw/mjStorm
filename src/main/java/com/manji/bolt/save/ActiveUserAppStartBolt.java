package com.manji.bolt.save;

import com.manji.utils.DateUtils;
import com.manji.utils.HbaseUtil;
import com.manji.utils.PerfixEnum;
import com.manji.utils.RowKeyUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: szw
 * Date: 2019/11/13
 * Time: 10:01
 */
public class ActiveUserAppStartBolt implements IRichBolt {
    static String CF = "cf1";
    String[] keys;
    String[] newKeys;
    String tableName;

    public ActiveUserAppStartBolt(String[] key, String tableName) {
        this.keys = key;
        this.tableName = tableName;
        this.newKeys = new String[keys.length];
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        if (input.getStringByField("event").equals("$AppStart")) {
            //替换成真实key
            for (int i = 0; i < keys.length; i++) {
                try {
                    newKeys[i] = input.getStringByField(keys[i]);
                } catch (Exception e) {
                    newKeys[i] = null;
                }
            }

            Long times = DateUtils.getTimeMills(input.getStringByField("times").replaceAll(PerfixEnum.DAY.getCode(), ""));
            for (String key : RowKeyUtil.getAllKeys(newKeys)) {
                try {
                    //保存时间
                    HbaseUtil.addRow(tableName, key, CF, "dataTime", times);
                    //启动次数
                    HbaseUtil.incrementColumnValue(tableName, key, CF, "startTime", 1L);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
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
