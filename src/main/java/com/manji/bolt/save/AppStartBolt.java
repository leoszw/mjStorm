package com.manji.bolt.save;

import com.alibaba.fastjson.JSONObject;
import com.manji.utils.DateUtils;
import com.manji.utils.HbaseUtil;
import com.manji.utils.PerfixEnum;
import com.manji.utils.RowKeyUtil;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: app启动数据统计（地域分布 && 版本分布 && 终端分布）
 * User: szw
 * Date: 2019-09-27
 * Time: 15:07
 */
public class AppStartBolt implements IRichBolt {
    static String CF = "cf1";
    String[] keys;
    String[] newKeys;
    String tableName;

    public AppStartBolt(String[] key, String tableName) {
        this.keys = key;
        this.tableName = tableName;
        this.newKeys = new String[keys.length];
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

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
                    //保存数据时间
                    if (StringUtils.isNotBlank(input.getStringByField("times")) && key.indexOf(input.getStringByField("times")) != -1) {
                        HbaseUtil.addRow(tableName, key, CF, "dataTime", times);

                        HbaseUtil.addRow(tableName, key + input.getStringByField("brand"), CF, "dataTime", times);
                        HbaseUtil.addRow(tableName, key + input.getStringByField("model"), CF, "dataTime", times);
                        HbaseUtil.addRow(tableName, key + input.getStringByField("networkType"), CF, "dataTime", times);
                        HbaseUtil.addRow(tableName, key + input.getStringByField("os"), CF, "dataTime", times);
                        HbaseUtil.addRow(tableName, key + input.getStringByField("resolutionRatio"), CF, "dataTime", times);
                    }

                    //启动次数
                    HbaseUtil.incrementColumnValue(tableName, key, CF, "startTime", 1L);

                    HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("brand"), CF, "startTime", 1L);
                    HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("model"), CF, "startTime", 1L);
                    HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("networkType"), CF, "startTime", 1L);
                    HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("os"), CF, "startTime", 1L);
                    HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("resolutionRatio"), CF, "startTime", 1L);

                    //启动用户数
                    if (key.indexOf(PerfixEnum.USER.getCode()) == -1) {
                        JSONObject json0 = HbaseUtil.getRow(tableName, key + input.getStringByField("userID"), null, null);
                        JSONObject json1 = HbaseUtil.getRow(tableName, key + input.getStringByField("userID") + input.getStringByField("brand"), null, null);
                        JSONObject json2 = HbaseUtil.getRow(tableName, key + input.getStringByField("userID") + input.getStringByField("model"), null, null);
                        JSONObject json3 = HbaseUtil.getRow(tableName, key + input.getStringByField("userID") + input.getStringByField("networkType"), null, null);
                        JSONObject json4 = HbaseUtil.getRow(tableName, key + input.getStringByField("userID") + input.getStringByField("os"), null, null);
                        JSONObject json5 = HbaseUtil.getRow(tableName, key + input.getStringByField("userID") + input.getStringByField("resolutionRatio"), null, null);

                        if (json0 == null)
                            HbaseUtil.incrementColumnValue(tableName, key, CF, "startUser", 1L);
                        if (json1 == null)
                            HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("brand"), CF, "startUser", 1L);
                        if (json2 == null)
                            HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("model"), CF, "startUser", 1L);
                        if (json3 == null)
                            HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("networkType"), CF, "startUser", 1L);
                        if (json4 == null)
                            HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("os"), CF, "startUser", 1L);
                        if (json5 == null)
                            HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("resolutionRatio"), CF, "startUser", 1L);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            //启动占比
//            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//            RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("[\\s\\S]+" + input.getStringByField("province") + "$"));
//            filterList.addFilter(rowFilter);
//
//            try {
//                List<JSONObject> datas = HbaseUtil.scanTable(tableName, filterList, new String[]{"startTime"}, new String[]{CF});
//                for (JSONObject json : datas) {
//                    Long allStartTime = HbaseUtil.getCellValue(tableName, json.getString("rowkey").replaceAll(input.getStringByField("province"), ""), CF, "startTime");
//                    HbaseUtil.addRow(tableName, json.getString("rowkey"), CF, "startTimeRatio", ComputeUtil.device(json.get("startTime"), allStartTime));
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//            //启动用户占比
//            try {
//                List<JSONObject> datas = HbaseUtil.scanTable(tableName, filterList, new String[]{"startUser"}, new String[]{CF});
//                for (JSONObject json : datas) {
//                    Long allStartUser = HbaseUtil.getCellValue(tableName, json.getString("rowkey").replaceAll(input.getStringByField("province"), ""), CF, "startUser");
//                    HbaseUtil.addRow(tableName, json.getString("rowkey"), CF, "startUserRatio", ComputeUtil.device(json.get("startUser"), allStartUser));
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
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
