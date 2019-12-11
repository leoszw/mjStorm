package com.manji.bolt.save;

import com.manji.utils.HbaseUtil;
import com.manji.utils.RowKeyUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 用户地域分布统计
 * User: szw
 * Date: 2019/11/21
 * Time: 15:38
 */
public class UserGeographyBolt implements IRichBolt {
    String tableName;
    String CF = "cf1";
    String[] keys;
    String[] newKeys;

    public UserGeographyBolt(String tableName, String[] keys) {
        this.tableName = tableName;
        this.keys = keys;
        this.newKeys = new String[keys.length];
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        //替换成真实key
        for (int i = 0; i < keys.length; i++) {
            try {
                newKeys[i] = input.getStringByField(keys[i]);
            } catch (NullPointerException e) {
                newKeys[i] = null;
            }
        }

        for (String key : RowKeyUtil.getAllKeys(newKeys)) {
            //启动次数 ，使用时长(新用户启动次数，启动用户数 因时间段有随机性，所以在查询时再统计)
            try {
                //数据时间
                HbaseUtil.addRow(tableName, key, CF, "dataTime", input.getStringByField("dataTime"));
                //启动次数
                HbaseUtil.incrementColumnValue(tableName, key, CF, "startTime", 1L);
                //使用时长
                Long useTime = HbaseUtil.getCellValue(tableName, key, CF, "useTime");
                useTime = null == useTime ? 0l : useTime;
                Long this_useTime = input.getLongByField("useTime");
                this_useTime = null == this_useTime ? 0l : this_useTime;
                HbaseUtil.addRow(tableName, key, CF, "useTime", useTime + this_useTime);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
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
