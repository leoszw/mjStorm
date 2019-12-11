package com.manji.bolt.other;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.UUID;

import static org.apache.storm.utils.Utils.tuple;

/**
 * 将应用端提交的批量Event数据拆分为单条Event数据
 */
public class SplitOriginalDataBolt implements IRichBolt {
    public void execute(Tuple input) {
        String msg = String.valueOf(input.getValues().get(0));
        try{
           JSONObject j = (JSONObject) JSON.parse(msg);
        }catch (Exception e){
            System.out.println(msg);
        }
        JSONObject jsonMsg = (JSONObject) JSON.parse(msg);
        String header = JSON.toJSONString(jsonMsg.get("header"));
        String body = JSON.toJSONString(jsonMsg.get("body"));
        String rowKey = "";
        if (body.startsWith("[")) {
            JSONArray jsonArray = JSON.parseArray(body);
            for (Object item : jsonArray) {
                body = JSON.toJSONString(item);
                rowKey = UUID.randomUUID().toString().replaceAll("-", "");
                collector.emit(tuple(rowKey, header, body));
            }
        } else {
            rowKey = UUID.randomUUID().toString().replaceAll("-", "");
            collector.emit(tuple(rowKey, header, body));
        }
    }

    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rowKey","header","body"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
