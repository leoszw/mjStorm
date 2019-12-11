package com.manji.bolt.save;

import com.alibaba.fastjson.JSONObject;
import com.manji.utils.ComputeUtil;
import com.manji.utils.HbaseUtil;
import com.manji.utils.RowKeyUtil;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 注册事件数据统计
 * User: szw
 * Date: 2019-10-14
 * Time: 11:02
 */
public class RegisterBolt implements IRichBolt {
    static String CF = "cf1";
    String[] keys;
    String[] newKeys;
    String tableName;

    public RegisterBolt(String[] keys, String tableName) {
        this.keys = keys;
        this.tableName = tableName;
        this.newKeys = new String[keys.length];
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        if (input.getStringByField("event").equals("$Register")) {
            //替换成真实key
            for (int i = 0; i < keys.length; i++) {
                try {
                    newKeys[i] = input.getStringByField(keys[i]);
                } catch (Exception e) {
                    newKeys[i] = null;
                }
            }

            //统计数据
            for (String key : RowKeyUtil.getAllKeys(newKeys)) {
                //注册数量统计（新用户）
                try {
                    HbaseUtil.incrementColumnValue(tableName, key, CF, "newUser", 1L);

                    HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("brand"), CF, "newUser", 1L);
                    HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("model"), CF, "newUser", 1L);
                    HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("networkType"), CF, "newUser", 1L);
                    HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("os"), CF, "newUser", 1L);
                    HbaseUtil.incrementColumnValue(tableName, key + input.getStringByField("resolutionRatio"), CF, "newUser", 1L);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            //新用户占比
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("[\\s\\S]+" + input.getStringByField("province") + "$"));
            filterList.addFilter(rowFilter);

            try {
                List<JSONObject> datas = HbaseUtil.scanTable(tableName, filterList, new String[]{"newUser"}, new String[]{CF});
                for (JSONObject json : datas) {
                    Long allNewUser = HbaseUtil.getCellValue(tableName, json.getString("rowkey").replaceAll(input.getStringByField("province"), ""), CF, "newUser");
                    HbaseUtil.addRow(tableName, json.getString("rowkey"), CF, "newUserRatio", ComputeUtil.device(json.get("newUser"), allNewUser));
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
