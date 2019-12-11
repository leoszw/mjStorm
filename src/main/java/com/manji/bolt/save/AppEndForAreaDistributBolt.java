package com.manji.bolt.save;

import com.manji.utils.ComputeUtil;
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
 * Description: app退出数据(地区分布，终端分布)统计
 * User: szw
 * Date: 2019-09-27
 * Time: 18:10
 */
public class AppEndForAreaDistributBolt implements IRichBolt {
    static String CF = "cf1";
    String[] keys;
    String[] newKeys;
    String tableName;

    public AppEndForAreaDistributBolt(String[] keys, String tableName) {
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
                try {
                    //使用时长
                    Long useTime = HbaseUtil.getCellValue(tableName, key, CF, "useTime");
                    if (useTime == null) useTime = 0l;
                    useTime += input.getLongByField("useTime");
                    HbaseUtil.addRow(tableName, key, CF, "useTime", useTime);

                    //终端分布统计数据
                    HbaseUtil.addRow(tableName, key + input.getStringByField("brand"), CF, "useTime", useTime);
                    HbaseUtil.addRow(tableName, key + input.getStringByField("model"), CF, "useTime", useTime);
                    HbaseUtil.addRow(tableName, key + input.getStringByField("networkType"), CF, "useTime", useTime);
                    HbaseUtil.addRow(tableName, key + input.getStringByField("os"), CF, "useTime", useTime);
                    HbaseUtil.addRow(tableName, key + input.getStringByField("resolutionRatio"), CF, "useTime", useTime);

                    //次均使用时长
                    Long startTime = HbaseUtil.getCellValue(tableName, key, CF, "startTime");
                    HbaseUtil.addRow(tableName, key, CF, "avgUseTime", startTime == null ? 0 : ComputeUtil.device((useTime + input.getDoubleByField("useTime")), startTime));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            //次均使用时长
//            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//            RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("[\\s\\S]+" + PerfixEnum.PROVINCE.getCode() + "[\\s\\S]+"));
//            filterList.addFilter(rowFilter);
//
//            try {
//                List<JSONObject> list = HbaseUtil.scanTable(tableName, filterList, new String[]{"useTime"}, new String[]{CF});
//                for (JSONObject json : list) {
//                    int per_index = json.getString("rowkey").indexOf(PerfixEnum.PROVINCE.getCode());
//                    int last_index = json.getString("rowkey").indexOf(PerfixEnum.USER.getCode());
//                    Long startTime = null;
//                    if (last_index == -1) {
//                        //没有用户信息，通过rowkey截取地区前部分来查询启动次数
//                        startTime = HbaseUtil.getCellValue(tableName, json.getString("rowkey").substring(0, per_index), CF, "startTime");
//                    } else {
//                        //有用户信息，截取rowkey地区前部分和后部分来查询启动次数
//                        startTime = HbaseUtil.getCellValue(tableName, json.getString("rowkey").substring(0, per_index) + json.getString("rowkey")
//                                .substring(last_index), CF, "startTime");
//                    }
//                    HbaseUtil.addRow(tableName, json.getString("rowkey"), CF, "avgUseTime", ComputeUtil.device(json.get("useTime"), startTime));
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }

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
