package com.manji.bolt.save;

import com.manji.utils.HbaseUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 页面分析
 * User: szw
 * Date: 2019/12/2
 * Time: 11:41
 */
public class SavePageAnalysisBolt implements IRichBolt {
    String tableName;
    String CF = "cf_infor";
    String appName, timestamp, hour, days, week, month, quarter, year, version, userGroup, screenName, title, fromScreen, event;

    public SavePageAnalysisBolt(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        appName = input.getStringByField("appName");
        timestamp = input.getStringByField("timestamp");
        hour = input.getStringByField("hour");
        days = input.getStringByField("days");
        week = input.getStringByField("week");
        month = input.getStringByField("month");
        quarter = input.getStringByField("quarter");
        year = input.getStringByField("year");
        version = input.getStringByField("version");
        userGroup = input.getStringByField("userGroup");
        screenName = input.getStringByField("screenName");
        title = input.getStringByField("title");
        event = input.getStringByField("event");

        try {
            //数据时间
            HbaseUtil.addRow(tableName, appName + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + screenName, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + year + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + year + quarter + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + year + month + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + year + week + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + days + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + days + hour + screenName, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + userGroup + year + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + userGroup + year + quarter + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + userGroup + year + month + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + userGroup + year + week + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + userGroup + days + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + userGroup + days + hour + screenName, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + userGroup + year + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + userGroup + year + quarter + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + userGroup + year + month + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + userGroup + year + week + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + userGroup + days + screenName, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + userGroup + days + hour + screenName, CF, "dataTime", timestamp);

            //页面标题
            if (StringUtils.isNotBlank(title)) {
                HbaseUtil.addRow(tableName, appName + screenName, CF, "title", title);
                //应用 + 年 。。。。
                HbaseUtil.addRow(tableName, appName + year + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + year + quarter + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + year + month + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + year + week + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + days + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + days + hour + screenName, CF, "title", title);

                HbaseUtil.addRow(tableName, appName + version + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + version + year + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + version + year + quarter + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + version + year + month + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + version + year + week + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + version + days + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + version + days + hour + screenName, CF, "title", title);

                HbaseUtil.addRow(tableName, appName + userGroup + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + userGroup + year + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + userGroup + year + quarter + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + userGroup + year + month + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + userGroup + year + week + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + userGroup + days + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + userGroup + days + hour + screenName, CF, "title", title);

                HbaseUtil.addRow(tableName, appName + version + userGroup + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + version + userGroup + year + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + version + userGroup + year + quarter + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + version + userGroup + year + month + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + version + userGroup + year + week + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + version + userGroup + days + screenName, CF, "title", title);
                HbaseUtil.addRow(tableName, appName + version + userGroup + days + hour + screenName, CF, "title", title);
            }

            //入口页次数
            if ("$AppStart".equals(event)) {
                HbaseUtil.incrementColumnValue(tableName, appName + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + screenName, CF, "inPage", 1L);

                HbaseUtil.incrementColumnValue(tableName, appName + version + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + year + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + year + quarter + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + year + month + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + year + week + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + days + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + days + hour + screenName, CF, "inPage", 1L);

                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + quarter + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + month + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + week + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + days + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + days + hour + screenName, CF, "inPage", 1L);

                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + quarter + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + month + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + week + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + days + screenName, CF, "inPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + days + hour + screenName, CF, "inPage", 1L);
            }
            //页面浏览次数
            if ("$AppViewScreen".equals(event) || "$AppStart".equals(event)) {
                HbaseUtil.incrementColumnValue(tableName, appName + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + screenName, CF, "viewCount", 1L);

                HbaseUtil.incrementColumnValue(tableName, appName + version + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + year + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + year + quarter + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + year + month + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + year + week + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + days + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + days + hour + screenName, CF, "viewCount", 1L);

                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + quarter + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + month + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + week + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + days + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + days + hour + screenName, CF, "viewCount", 1L);

                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + quarter + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + month + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + week + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + days + screenName, CF, "viewCount", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + days + hour + screenName, CF, "viewCount", 1L);
            }
            //退出页次数
            if ("$AppEnd".equals(event)) {
                HbaseUtil.incrementColumnValue(tableName, appName + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + screenName, CF, "exitPage", 1L);

                HbaseUtil.incrementColumnValue(tableName, appName + version + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + year + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + year + quarter + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + year + month + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + year + week + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + days + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + days + hour + screenName, CF, "exitPage", 1L);

                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + quarter + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + month + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + week + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + days + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + days + hour + screenName, CF, "exitPage", 1L);

                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + quarter + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + month + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + week + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + days + screenName, CF, "exitPage", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + days + hour + screenName, CF, "exitPage", 1L);
            }
            // todo 页面停留时间未统计
        } catch (IOException e) {
            e.printStackTrace();
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
