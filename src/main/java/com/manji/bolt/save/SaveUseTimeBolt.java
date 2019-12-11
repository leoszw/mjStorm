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
 * Description: 保存访问时长 completed
 * User: szw
 * Date: 2019/11/26
 * Time: 10:53
 */
public class SaveUseTimeBolt implements IRichBolt {
    String tableName = null;
    String CF = "cf_infor";
    boolean versionAndUserGroupDimension = false;
    boolean areaDimension = false;
    String appName, year, quarter, month, week, days, hour, version, userGroup,
            province, brand, model, os, resolutionRatio, networkType;
    Long useTime = null;

    public SaveUseTimeBolt(String tableName) {
        this.tableName = tableName;
    }

    public SaveUseTimeBolt(String tableName, Boolean versionAndUserGroupDimension, Boolean areaDimension) {
        this.tableName = tableName;
        this.versionAndUserGroupDimension = versionAndUserGroupDimension;
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
        version = input.getStringByField("version");
        userGroup = input.getStringByField("userGroup");
        useTime = input.getLongByField("useTime");
        province = input.getStringByField("province");

        try {
            HbaseUtil.incrementColumnValue(tableName, appName, CF, "useTime", useTime);
            HbaseUtil.incrementColumnValue(tableName, appName + year, CF, "useTime", useTime);
            HbaseUtil.incrementColumnValue(tableName, appName + year + quarter, CF, "useTime", useTime);
            HbaseUtil.incrementColumnValue(tableName, appName + year + month, CF, "useTime", useTime);
            HbaseUtil.incrementColumnValue(tableName, appName + year + week, CF, "useTime", useTime);
            HbaseUtil.incrementColumnValue(tableName, appName + days, CF, "useTime", useTime);
            HbaseUtil.incrementColumnValue(tableName, appName + days + hour, CF, "useTime", useTime);

            //是否统计版本用户组维度
            if (versionAndUserGroupDimension) {

                brand = input.getStringByField("brand");
                model = input.getStringByField("model");
                os = input.getStringByField("os");
                resolutionRatio = input.getStringByField("resolutionRatio");
                networkType = input.getStringByField("networkType");

                HbaseUtil.incrementColumnValue(tableName, appName + version, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + userGroup, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup, CF, "useTime", useTime);

                //是否统计地区维度
                if (areaDimension && StringUtils.isNotBlank(province)) {
                    HbaseUtil.incrementColumnValue(tableName, appName + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + month + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + week + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + days + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + days + hour + province, CF, "useTime", useTime);

                    HbaseUtil.incrementColumnValue(tableName, appName + version + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + version + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + days + version + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + province, CF, "useTime", useTime);

                    HbaseUtil.incrementColumnValue(tableName, appName + userGroup + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup + province, CF, "useTime", useTime);

                    HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup + province, CF, "useTime", useTime);
                    HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup + province, CF, "useTime", useTime);
                }

                //设备型号等维度
                //品牌
                HbaseUtil.incrementColumnValue(tableName, appName + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + brand, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + version + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + brand, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup + brand, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup + brand, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup + brand, CF, "useTime", useTime);

                //设备型号
                HbaseUtil.incrementColumnValue(tableName, appName + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + model, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + version + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + model, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup + model, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup + model, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup + model, CF, "useTime", useTime);

                //操作系统
                HbaseUtil.incrementColumnValue(tableName, appName + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + os, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + version + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + os, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup + os, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup + os, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup + os, CF, "useTime", useTime);

                //分辨率
                HbaseUtil.incrementColumnValue(tableName, appName + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + resolutionRatio, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + version + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + resolutionRatio, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup + resolutionRatio, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup + resolutionRatio, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup + resolutionRatio, CF, "useTime", useTime);

                //联网方式
                HbaseUtil.incrementColumnValue(tableName, appName + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + networkType, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + version + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + networkType, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + userGroup + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + userGroup + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + userGroup + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + userGroup + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + userGroup + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + userGroup + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + userGroup + networkType, CF, "useTime", useTime);

                HbaseUtil.incrementColumnValue(tableName, appName + version + userGroup + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + version + userGroup + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + quarter + version + userGroup + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + month + version + userGroup + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + year + week + version + userGroup + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + version + userGroup + networkType, CF, "useTime", useTime);
                HbaseUtil.incrementColumnValue(tableName, appName + days + hour + version + userGroup + networkType, CF, "useTime", useTime);
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
