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
 * Description: 页面停留数据统计
 * User: szw
 * Date: 2019-09-27
 * Time: 18:10
 */
public class WebStayBolt implements IRichBolt {
    static String CF = "cf1";
    String[] keys;
    String[] newKeys;
    String tableName;

    public WebStayBolt(String[] keys, String tableName) {
        this.keys = keys;
        this.tableName = tableName;
        this.newKeys = new String[keys.length];
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    public void execute(Tuple input) {
        if (input.getStringByField("event").equals("$WebStay")) {
            //替换成真实key
            for (int i = 0; i < keys.length; i++) {
                try {
                    newKeys[i] = input.getStringByField(keys[i]);
                } catch (Exception e) {
                    newKeys[i] = null;
                }
            }

            //循环统计
            for (String key : RowKeyUtil.getAllKeys(newKeys)) {
                //停留时间统计
                Double stayTime = null;
                try {
                    stayTime = HbaseUtil.getCellValue(tableName, key, CF, "stayTime");
                } catch (Exception e) {

                }
                if (stayTime == null) stayTime = new Double(0);
                try {
                    stayTime += input.getDoubleByField("pageStay");
                    HbaseUtil.addRow(tableName, key, CF, "stayTime", stayTime);
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                //平均停留时间统计
                try {
                    Long viewCount = HbaseUtil.getCellValue(tableName, key, CF, "viewCount");
                    Double stayTimes = HbaseUtil.getCellValue(tableName, key, CF, "stayTime");

                    HbaseUtil.addRow(tableName, key, CF, "stayTimeAvg", ComputeUtil.device(stayTimes, viewCount));

                    //停留时间占比
//                    List<JSONObject> jsons= HbaseUtil.scanTable("t1",null,new String[]{"stayTime"},new String[]{"cf1"});
//
//                    Double allStayTime = null;
//                    for (JSONObject json:jsons) {
//                        allStayTime = HbaseUtil.getCellValue(tableName,json.getString("rowkey").replaceAll("INDEXPAGE[\\s\\S]+$",""), CF, "stayTime");
//                        if(allStayTime != null){
//                            HbaseUtil.addRow(tableName, json.getString("rowkey"), CF, "stayTimeRatio", ComputeUtil.device(json.get("stayTime"), allStayTime));
//                        }
//                    }
                    Double allStayTime = HbaseUtil.getCellValue(tableName, key.replaceAll(input.getStringByField("urlPath"), ""),
                            CF, "stayTime");

                    HbaseUtil.addRow(tableName, key, CF, "stayTimeRatio", ComputeUtil.device(stayTimes, allStayTime));
                } catch (Exception e) {
                    e.printStackTrace();
                }

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
