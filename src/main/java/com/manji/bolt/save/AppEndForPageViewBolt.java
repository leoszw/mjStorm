package com.manji.bolt.save;

import com.manji.utils.ComputeUtil;
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
 * Description: app退出数据(页面浏览)统计
 * User: szw
 * Date: 2019-09-27
 * Time: 18:10
 */
public class AppEndForPageViewBolt implements IRichBolt {
    static String CF = "cf1";
    String[] keys;
    String[] newKeys;
    String tableName;

    public AppEndForPageViewBolt(String[] keys, String tableName) {
        this.keys = keys;
        this.tableName = tableName;
        this.newKeys = new String[keys.length];
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

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

            //循环统计
            for (String key : RowKeyUtil.getAllKeys(newKeys)) {
                //页面退出次数统计
//                if(StringUtils.isNotBlank(input.getStringByField("referrerPath")) || key.indexOf(input.getStringByField("referrerPath")) == -1){
                try {
                    HbaseUtil.incrementColumnValue(tableName, key, CF, "exitCount", 1L);
                } catch (IOException e) {
                    e.printStackTrace();
                }
//                }

                //退出率统计
//                if(StringUtils.isBlank(input.getStringByField("referrerPath")) || key.indexOf(input.getStringByField("referrerPath")) == -1){
                try {
                    Long viewCount = HbaseUtil.getCellValue(tableName, key, CF, "viewCount");
                    Long exitCount = HbaseUtil.getCellValue(tableName, key, CF, "exitCount");

                    HbaseUtil.addRow(tableName, key, CF, "exitRatio", ComputeUtil.device(exitCount, viewCount));
                } catch (Exception e) {
                    e.printStackTrace();
                }
//                }

            }
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
