package com.manji.bolt.save;

import com.manji.utils.HbaseUtil;
import com.manji.utils.RowKeyUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 用户趋势统计使用时长
 * User: szw
 * Date: 2019/11/11
 * Time: 14:54
 */
public class UserTrendAppEndBolt implements IRichBolt {
    static String CF = "cf1";
    String[] keys;
    String[] newKeys;
    String tableName;

    public UserTrendAppEndBolt(String[] keys, String tableName) {
        this.keys = keys;
        this.tableName = tableName;
        this.newKeys = new String[keys.length];
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        if (input.getStringByField("event").equals("$AppEnd")) {
            //替换成真实key
            for (int i = 0; i < keys.length; i++) {
                try {
                    newKeys[i] = input.getStringByField(keys[i]);
                } catch (NullPointerException e) {
                    newKeys[i] = null;
                }
            }

            /**
             * 统计使用时长
             * 1.获取原来的时长
             * 2.加上本次的时长
             * 3.存回hbase
             */
            for (String key : RowKeyUtil.getAllKeys(newKeys)) {
                try {
                    Long useTime = HbaseUtil.getCellValue(tableName, key, CF, "useTime");
                    useTime = useTime == null ? 0l : useTime;
                    useTime += input.getLongByField("useTime");
                    HbaseUtil.addRow(tableName, key, CF, "useTime", useTime);
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
