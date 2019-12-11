package com.manji.bolt.save;

import com.manji.utils.HbaseUtil;
import com.manji.utils.RowKeyUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: app升级数据
 * User: szw
 * Date: 2019-09-27
 * Time: 15:07
 */
public class UpdateAppBolt implements IRichBolt {
    static String CF = "cf1";
    String[] keys;
    String[] newKeys;
    String tableName;

    public UpdateAppBolt(String[] key, String tableName) {
        this.keys = key;
        this.tableName = tableName;
        this.newKeys = new String[keys.length];
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    public void execute(Tuple input) {
        if (input.getStringByField("event").equals("$UpdateApp")) {
            //替换成真实key
            for (int i = 0; i < keys.length; i++) {
                try {
                    newKeys[i] = input.getStringByField(keys[i]);
                } catch (Exception e) {
                    newKeys[i] = null;
                }
            }

            for (String key : RowKeyUtil.getAllKeys(newKeys)) {
                try {
                    //升级用户数
                    HbaseUtil.incrementColumnValue(tableName, key, CF, "upUser", 1L);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("a"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
