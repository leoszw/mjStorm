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
 * Description: 保存启动用户数 completed
 * User: szw
 * Date: 2019/11/27
 * Time: 10:59
 */
public class SaveStartUserBolt implements IRichBolt {
    String tableName = null;
    String CF = "cf_infor";
    boolean areaDimension = false;
    String appName, year, quarter, month, week, days, hour, timestamp, version, userGroup, distinctId,
            province, brand, model, os, resolutionRatio, networkType, registerTime;

    public SaveStartUserBolt(String tableName) {
        this.tableName = tableName;
    }

    public SaveStartUserBolt(String tableName, boolean areaDimension) {
        this.tableName = tableName;
        this.areaDimension = areaDimension;
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
        distinctId = input.getStringByField("distinctId");
        province = input.getStringByField("province");

        brand = input.getStringByField("brand");
        model = input.getStringByField("model");
        os = input.getStringByField("os");
        resolutionRatio = input.getStringByField("resolutionRatio");
        networkType = input.getStringByField("networkType");

        registerTime = input.getStringByField("registerTime");

        try {
            /** 数据时间 **/
            HbaseUtil.addRow(tableName, appName + distinctId, CF, "dataTime", timestamp);
            //应用 + 年 。。。。
            HbaseUtil.addRow(tableName, appName + year + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + userGroup + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + userGroup + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + distinctId, CF, "dataTime", timestamp);

            //是否统计地区维度
            if (areaDimension && StringUtils.isNotBlank(province)) {
                HbaseUtil.addRow(tableName, appName + province + distinctId, CF, "dataTime", timestamp);
                //应用 + 年 。。。。
                HbaseUtil.addRow(tableName, appName + year + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + quarter + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + month + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + week + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + hour + province + distinctId, CF, "dataTime", timestamp);

                HbaseUtil.addRow(tableName, appName + version + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + version + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + month + version + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + week + version + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + version + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + hour + version + province + distinctId, CF, "dataTime", timestamp);

                HbaseUtil.addRow(tableName, appName + userGroup + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + userGroup + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + month + userGroup + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + week + userGroup + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + userGroup + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + hour + userGroup + province + distinctId, CF, "dataTime", timestamp);

                HbaseUtil.addRow(tableName, appName + version + userGroup + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + version + userGroup + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + version + userGroup + province + distinctId, CF, "dataTime", timestamp);
                HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + province + distinctId, CF, "dataTime", timestamp);
            }

            //设备型号等维度
            //品牌
            HbaseUtil.addRow(tableName, appName + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + brand + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + brand + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + userGroup + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + userGroup + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + userGroup + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + userGroup + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + userGroup + brand + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + userGroup + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + userGroup + brand + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + brand + distinctId, CF, "dataTime", timestamp);

            //设备型号
            HbaseUtil.addRow(tableName, appName + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + model + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + model + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + userGroup + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + userGroup + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + userGroup + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + userGroup + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + userGroup + model + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + userGroup + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + userGroup + model + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + model + distinctId, CF, "dataTime", timestamp);

            //操作系统
            HbaseUtil.addRow(tableName, appName + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + os + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + os + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + userGroup + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + userGroup + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + userGroup + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + userGroup + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + userGroup + os + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + userGroup + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + userGroup + os + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + os + distinctId, CF, "dataTime", timestamp);

            //分辨率
            HbaseUtil.addRow(tableName, appName + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + resolutionRatio + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + resolutionRatio + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + resolutionRatio + distinctId, CF, "dataTime", timestamp);

            //联网方式
            HbaseUtil.addRow(tableName, appName + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + networkType + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + networkType + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + userGroup + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + userGroup + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + userGroup + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + userGroup + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + userGroup + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + userGroup + networkType + distinctId, CF, "dataTime", timestamp);

            HbaseUtil.addRow(tableName, appName + version + userGroup + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + version + userGroup + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + version + userGroup + networkType + distinctId, CF, "dataTime", timestamp);
            HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + networkType + distinctId, CF, "dataTime", timestamp);

            /** 注册时间 **/
            if (null != registerTime) {
                HbaseUtil.addRow(tableName, appName + distinctId, CF, "registerTime", registerTime);
                //应用 + 年 。。。。
                HbaseUtil.addRow(tableName, appName + year + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + version + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + version + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + version + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + version + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + version + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + version + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + userGroup + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + userGroup + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + userGroup + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + userGroup + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + userGroup + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + userGroup + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + version + userGroup + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + version + userGroup + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + version + userGroup + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + distinctId, CF, "registerTime", registerTime);

                //是否统计地区维度
                if (areaDimension && StringUtils.isNotBlank(province)) {
                    HbaseUtil.addRow(tableName, appName + province + distinctId, CF, "registerTime", registerTime);
                    //应用 + 年 。。。。
                    HbaseUtil.addRow(tableName, appName + year + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + quarter + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + month + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + week + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + days + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + days + hour + province + distinctId, CF, "registerTime", registerTime);

                    HbaseUtil.addRow(tableName, appName + version + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + version + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + quarter + version + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + month + version + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + week + version + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + days + version + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + days + hour + version + province + distinctId, CF, "registerTime", registerTime);

                    HbaseUtil.addRow(tableName, appName + userGroup + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + userGroup + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + month + userGroup + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + week + userGroup + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + days + userGroup + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + days + hour + userGroup + province + distinctId, CF, "registerTime", registerTime);

                    HbaseUtil.addRow(tableName, appName + version + userGroup + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + version + userGroup + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + days + version + userGroup + province + distinctId, CF, "registerTime", registerTime);
                    HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + province + distinctId, CF, "registerTime", registerTime);
                }

                //设备型号等维度
                //品牌
                HbaseUtil.addRow(tableName, appName + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + brand + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + version + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + version + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + version + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + version + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + version + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + version + brand + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + userGroup + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + userGroup + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + userGroup + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + userGroup + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + userGroup + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + userGroup + brand + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + version + userGroup + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + version + userGroup + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + version + userGroup + brand + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + brand + distinctId, CF, "registerTime", registerTime);

                //设备型号
                HbaseUtil.addRow(tableName, appName + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + model + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + version + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + version + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + version + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + version + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + version + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + version + model + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + userGroup + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + userGroup + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + userGroup + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + userGroup + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + userGroup + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + userGroup + model + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + version + userGroup + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + version + userGroup + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + version + userGroup + model + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + model + distinctId, CF, "registerTime", registerTime);

                //操作系统
                HbaseUtil.addRow(tableName, appName + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + os + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + version + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + version + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + version + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + version + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + version + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + version + os + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + userGroup + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + userGroup + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + userGroup + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + userGroup + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + userGroup + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + userGroup + os + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + version + userGroup + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + version + userGroup + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + version + userGroup + os + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + os + distinctId, CF, "registerTime", registerTime);

                //分辨率
                HbaseUtil.addRow(tableName, appName + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + resolutionRatio + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + version + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + version + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + version + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + version + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + version + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + version + resolutionRatio + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + version + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + version + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + version + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + resolutionRatio + distinctId, CF, "registerTime", registerTime);

                //联网方式
                HbaseUtil.addRow(tableName, appName + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + networkType + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + version + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + version + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + version + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + version + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + version + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + version + networkType + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + userGroup + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + userGroup + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + userGroup + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + userGroup + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + userGroup + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + userGroup + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + userGroup + networkType + distinctId, CF, "registerTime", registerTime);

                HbaseUtil.addRow(tableName, appName + version + userGroup + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + version + userGroup + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + quarter + version + userGroup + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + month + version + userGroup + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + year + week + version + userGroup + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + version + userGroup + networkType + distinctId, CF, "registerTime", registerTime);
                HbaseUtil.addRow(tableName, appName + days + hour + version + userGroup + networkType + distinctId, CF, "registerTime", registerTime);
            }
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
