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
 * Description: 保存app启动次数
 * User: szw
 * Date: 2019/11/27
 * Time: 9:31
 */
public class SaveStartTimesBolt implements IRichBolt {
    String tableName;
    String CF = "cf_infor";
    String appName, year, quarter, month, week, days, hour, timestamp, version,
            userGroup, province, brand, model, os, resolutionRatio, networkType, distinctId;

    public SaveStartTimesBolt(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        appName = input.getStringByField("appName");
        year = input.getStringByField("year");
        quarter = input.getStringByField("quarter");
        month = input.getStringByField("month");
        week = input.getStringByField("week");
        days = input.getStringByField("days");
        hour = input.getStringByField("hour");
        timestamp = input.getStringByField("timestamp");
        version = input.getStringByField("version");
        userGroup = input.getStringByField("userGroup");

        brand = input.getStringByField("brand");
        model = input.getStringByField("model");
        os = input.getStringByField("os");
        resolutionRatio = input.getStringByField("resolutionRatio");
        networkType = input.getStringByField("networkType");

        distinctId = input.getStringByField("distinctId");

        try {
            //数据时间
            //应用维度
            HbaseUtil.addRow(tableName, appName, CF, "dataTime", timestamp);
            //应用 + 年 。。。。
            HbaseUtil.addRow(tableName, appName + year, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + userGroup, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + userGroup, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + userGroup, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + userGroup, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + userGroup, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + userGroup, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + userGroup, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + userGroup, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + userGroup, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + userGroup, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup, CF, "dataTime", timestamp);

            if(StringUtils.isNotBlank(province)) {
                HbaseUtil.addRow(tableName, appName + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + quarter + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + month + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + week + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + hour + province, CF, "dataTime", timestamp);

                HbaseUtil.addRow(tableName, appName + version + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + version + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + month + version + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + week + version + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + version + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + hour + version + province, CF, "dataTime", timestamp);

                HbaseUtil.addRow(tableName, appName + userGroup + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + userGroup + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + month + userGroup + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + week + userGroup + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + userGroup + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + hour + userGroup + province, CF, "dataTime", timestamp);

                HbaseUtil.addRow(tableName, appName + version + userGroup + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + version + userGroup + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + version + userGroup + province, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + province, CF, "dataTime", timestamp);
            }

            HbaseUtil.addRow(tableName, appName + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + brand, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + brand, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + userGroup + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + userGroup + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + userGroup + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + userGroup + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + userGroup + brand, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + userGroup + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + userGroup + brand, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + brand, CF, "dataTime", timestamp);

            //设备型号
            HbaseUtil.addRow(tableName, appName + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + model, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + model, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + userGroup + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + userGroup + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + userGroup + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + userGroup + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + userGroup + model, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + userGroup + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + userGroup + model, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + model, CF, "dataTime", timestamp);

            //操作系统
            HbaseUtil.addRow(tableName, appName + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + os, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + os, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + userGroup + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + userGroup + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + userGroup + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + userGroup + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + userGroup + os, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + userGroup + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + userGroup + os, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + os, CF, "dataTime", timestamp);

            //分辨率
            HbaseUtil.addRow(tableName, appName + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + resolutionRatio, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + resolutionRatio, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + userGroup + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + userGroup + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + userGroup + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + userGroup + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + userGroup + resolutionRatio, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + userGroup + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + userGroup + resolutionRatio, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + resolutionRatio, CF, "dataTime", timestamp);

            //联网方式
            HbaseUtil.addRow(tableName, appName + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + networkType, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + networkType, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + userGroup + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + userGroup + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + userGroup + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + userGroup + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + userGroup + networkType, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + userGroup + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + userGroup + networkType, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + networkType, CF, "dataTime", timestamp);

            //启动次数
            HbaseUtil.incrementColumnValue(tableName, appName, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + version, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + version, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + version, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + version, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + userGroup, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup, CF, "startTimes", 1L);

            if(StringUtils.isNotBlank(province)) {
                HbaseUtil.incrementColumnValue(tableName, appName + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + province, CF, "startTimes", 1L);

                HbaseUtil.incrementColumnValue(tableName, appName + version + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + province, CF, "startTimes", 1L);

                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup + province, CF, "startTimes", 1L);

                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup + province, CF, "startTimes", 1L);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup + province, CF, "startTimes", 1L);
            }

            //设备型号等维度
            //品牌
            HbaseUtil.incrementColumnValue(tableName, appName + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + brand, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + version + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + version + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + brand, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + userGroup + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup + brand, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup + brand, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup + brand, CF, "startTimes", 1L);

            //设备型号
            HbaseUtil.incrementColumnValue(tableName, appName + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + model, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + version + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + version + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + model, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + userGroup + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup + model, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup + model, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup + model, CF, "startTimes", 1L);

            //操作系统
            HbaseUtil.incrementColumnValue(tableName, appName + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + os, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + version + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + version + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + os, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + userGroup + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup + os, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup + os, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup + os, CF, "startTimes", 1L);

            //分辨率
            HbaseUtil.incrementColumnValue(tableName, appName + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + resolutionRatio, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + version + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + version + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + resolutionRatio, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + userGroup + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup + resolutionRatio, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup + resolutionRatio, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup + resolutionRatio, CF, "startTimes", 1L);

            //联网方式
            HbaseUtil.incrementColumnValue(tableName, appName + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + networkType, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + version + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + version + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + networkType, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + userGroup + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup + networkType, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup + networkType, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup + networkType, CF, "startTimes", 1L);

            //每个用户的启动次数 和 数据时间
            HbaseUtil.addRow(tableName, appName + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + year + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + year + quarter + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + year + month + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + year + week + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + days + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + days + hour + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + userGroup + year + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + userGroup + year + quarter + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + userGroup + year + month + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + userGroup + year + week + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + userGroup + days + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + userGroup + days + hour + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + userGroup + year + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + userGroup + year + quarter + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + userGroup + year + month + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + userGroup + year + week + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + userGroup + days + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + version + userGroup + days + hour + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.incrementColumnValue(tableName, appName + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour + distinctId, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + version + year + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + version + year + quarter + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + version + year + month + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + version + year + week + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + version + days + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + version + days + hour + distinctId, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + userGroup + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + quarter + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + month + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + userGroup + year + week + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + userGroup + days + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + userGroup + days + hour + distinctId, CF, "startTimes", 1L);

            HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + quarter + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + month + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + year + week + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + days + distinctId, CF, "startTimes", 1L);
            HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + days + hour + distinctId, CF, "startTimes", 1L);
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
