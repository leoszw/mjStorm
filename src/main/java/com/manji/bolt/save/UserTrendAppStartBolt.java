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
 * Description: 用户趋势新老用户统计
 * User: szw
 * Date: 2019-09-27
 * Time: 18:10
 */
public class UserTrendAppStartBolt implements IRichBolt {
    static String CF = "cf1";
    String[] keys;
    String[] newKeys;
    String tableName;

    public UserTrendAppStartBolt(String[] keys, String tableName) {
        this.keys = keys;
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
                } catch (NullPointerException e) {
                    newKeys[i] = null;
                }
            }

            //用户趋势--新用户数统计
            //先判断当天是否统计过，没有统计过就统计用户数，统计过了就不统计用户数，只统计启动次数
            boolean isNewUser = "是第一次启动APP 并且 用户注册时间是数据时间（当天）".equals("");

            try {
                Long startTime = HbaseUtil.getCellValue(tableName, "时间+用户标识", CF, "startTime");
                for (String key : RowKeyUtil.getAllKeys(newKeys)) {
                    //当天没有统计过
                    if (null == startTime || startTime == 0) {
                        if (isNewUser) {
                            //新用户
                            HbaseUtil.incrementColumnValue(tableName, key, CF, "newUser", 1L);
                        } else {
                            //老用戶
                            HbaseUtil.incrementColumnValue(tableName, key, CF, "oldUser", 1L);
                        }
                        //启动用户
                        HbaseUtil.incrementColumnValue(tableName, key, CF, "startUser", 1L);
                    }
                    //统计启动次数
                    HbaseUtil.incrementColumnValue(tableName, key, CF, "startTime", 1L);
                }
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
