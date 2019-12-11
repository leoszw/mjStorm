package com.manji.bolt.save;

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

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 页面浏览数据统计
 * User: szw
 * Date: 2019-09-27
 * Time: 15:07
 */
public class PageViewBolt implements IRichBolt {
    static String CF = "cf1";
    String[] keys;
    String[] newKeys;
    String tableName;

    public PageViewBolt(String[] key, String tableName) {
        this.keys = key;
        this.tableName = tableName;
        this.newKeys = new String[keys.length];
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    public void execute(Tuple input) {
        if (input.getStringByField("event").equals("$pageview")) {
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
                    //页面浏览次数统计
                    HbaseUtil.incrementColumnValue(tableName, key, CF, "viewCount", 1L);

                    //页面跳转次数统计,跳转占比
                    if (key.indexOf(input.getStringByField("urlPath")) != -1 &&
                            StringUtils.isNotBlank(input.getStringByField("referrerPath")) &&
                            key.indexOf(input.getStringByField("referrerPath")) != -1) {
                        //跳转次数
                        HbaseUtil.incrementColumnValue(tableName, key, CF, "jumpCount", 1L);
                        HbaseUtil.incrementColumnValue(tableName, key.replaceAll(input.getStringByField("urlPath"), ""), CF, "allJump", 1L);
//                        //跳转占比
//                        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//                        RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("[\\s\\S]+"+input.getStringByField("referrerPath")+"$"));
//                        filterList.addFilter(rowFilter);
//                        List<JSONObject> jsons= HbaseUtil.scanTable("t1",filterList,new String[]{"jumpCount"},new String[]{"cf1"});
//
//                        Long allJump = null;
//                        for (JSONObject json:jsons) {
//                            allJump = HbaseUtil.getCellValue(tableName,json.getString("rowkey").replaceAll("INDEXPAGE:[\\s\\S]+SUPERPAGE", "SUPERPAGE"), CF, "allJump");
//                            HbaseUtil.addRow(tableName, json.getString("rowkey"), CF, "jumpRatio", ComputeUtil.device(json.get("jumpCount"), allJump));
//                        }
                    }

                    //保存数据时间
                    if (StringUtils.isNotBlank(input.getStringByField("times")) && key.indexOf(input.getStringByField("times")) != -1) {
                        HbaseUtil.addRow(tableName, key, CF, "dataTime",
                                DateUtils.getTimeMills(input.getStringByField("times").replaceAll(PerfixEnum.DAY.getCode(), "")));
                    }

                    if (key.indexOf(input.getStringByField("urlPath")) != -1) {
                        //保存页面路径
                        HbaseUtil.addRow(tableName, key, CF, "urlPath",
                                input.getStringByField("urlPath").replaceAll(PerfixEnum.INDEXPAGE.getCode(), ""));
                        //页面名称
                        HbaseUtil.addRow(tableName, key, CF, "title", input.getStringByField("title"));
                    }

//                    //统计访问占比
//                    try {
//                        if(key.indexOf(input.getStringByField("urlPath")) != -1){
//                            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//                            RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("[\\s\\S]+"+input.getStringByField("urlPath")));
//                            filterList.addFilter(rowFilter);
//                            List<JSONObject> jsons= HbaseUtil.scanTable("t1",filterList,new String[]{"viewCount"},new String[]{"cf1"});
//
//                            Long allCount = null;
//                            for (JSONObject json:jsons) {
//                                allCount = HbaseUtil.getCellValue(tableName,json.getString("rowkey").replaceAll("INDEXPAGE[\\s\\S]+$",""), CF, "viewCount");
//                                HbaseUtil.addRow(tableName, json.getString("rowkey"), CF, "viewRatio", ComputeUtil.device(json.get("viewCount"), allCount));
//                            }
//                        }
//
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }

                    //页面跳转信息
                    if (StringUtils.isNotBlank(input.getStringByField("referrerPath")) && key.indexOf(input.getStringByField("referrerPath")) != -1) {

                        //上级页面路径和名称
                        HbaseUtil.addRow(tableName, key, CF, "jumpFrom", input.getStringByField("referrerPath").replaceAll(PerfixEnum.SUPERPAGE.getCode(), ""));
                        HbaseUtil.addRow(tableName, key, CF, "jumpFromName", input.getStringByField("referrerName"));
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            //统计访问占比
//            try {
//                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//                RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("[\\s\\S]+" + input.getStringByField("urlPath")));
//                filterList.addFilter(rowFilter);
//                List<JSONObject> jsons = HbaseUtil.scanTable("t1", filterList, new String[]{"viewCount"}, new String[]{"cf1"});
//
//                Long allCount = null;
//                for (JSONObject json : jsons) {
//                    allCount = HbaseUtil.getCellValue(tableName, json.getString("rowkey").replaceAll("INDEXPAGE[\\s\\S]+$", ""), CF, "viewCount");
//                    HbaseUtil.addRow(tableName, json.getString("rowkey"), CF, "viewRatio", ComputeUtil.device(json.get("viewCount"), allCount));
//                }
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }

            //跳转占比
//            try {
//                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//                RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("[\\s\\S]+" + input.getStringByField("referrerPath") + "$"));
//                filterList.addFilter(rowFilter);
//                List<JSONObject> jsons = null;
//
//                jsons = HbaseUtil.scanTable("t1", filterList, new String[]{"jumpCount"}, new String[]{"cf1"});
//
//                Long allJump = null;
//                for (JSONObject json : jsons) {
//                    allJump = HbaseUtil.getCellValue(tableName, json.getString("rowkey").replaceAll("INDEXPAGE:[\\s\\S]+SUPERPAGE", "SUPERPAGE"), CF, "allJump");
//                    HbaseUtil.addRow(tableName, json.getString("rowkey"), CF, "jumpRatio", ComputeUtil.device(json.get("jumpCount"), allJump));
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
