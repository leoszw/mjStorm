package com.manji.bolt.save;

import com.manji.utils.HbaseUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 保存用户信息
 * User: szw
 * Date: 2019/11/19
 * Time: 16:26
 */
public class SaveUserInfoBolt implements IRichBolt {

    OutputCollector collector = null;
    String tableName = null;
    String CF = "cf1";

    public SaveUserInfoBolt(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        //用户信息保存到hbase

        //TODO 已有用户信息如何同步到hbase，用户信息完善需同步？

        String userName = input.getStringByField("userName");

        if (null != userName) {
            try {
                //注册时间
                HbaseUtil.addRow(tableName, userName, CF, "registryTime", input.getStringByField("registryTime"));
                //性别
                HbaseUtil.addRow(tableName, userName, CF, "sex", input.getStringByField("sex"));
                //年龄
                HbaseUtil.addRow(tableName, userName, CF, "age", input.getStringByField("age"));
                //地区
                HbaseUtil.addRow(tableName, userName, CF, "area", input.getStringByField("area"));
                //学历
                HbaseUtil.addRow(tableName, userName, CF, "education", input.getStringByField("education"));
                //爱好
                HbaseUtil.addRow(tableName, userName, CF, "interest", input.getStringByField("interest"));
                //职业
                HbaseUtil.addRow(tableName, userName, CF, "job", input.getStringByField("job"));
            } catch (IOException e) {
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
