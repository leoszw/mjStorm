package com.manji.bolt.filter;

import com.alibaba.fastjson.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 根据事件名称过滤数 据
 * User: szw
 * Date: 2019-09-28
 * Time: 17:10
 */
public class SpoutFilterBolt implements IRichBolt {
    OutputCollector collector;
    JSONObject jsonObject;
    String body;
    String rowkey;
    String header;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

//    @Override
//    public void execute(Tuple input) {
//        body = input.getString(0);
//
//        try {
//            if (body.startsWith("[")) {
//                jsonObject = JSONObject.parseArray(body).getJSONObject(0);
//            } else {
//                jsonObject = JSONObject.parseObject(body);
//            }
//        } catch (Exception e) {
//            //解析json异常
//        }
//        if (jsonObject.getString("event") != null && jsonObject.getString("event").equals(event)) {
//            collector.emit(new Values("",body));
//        }
//    }

    String event = null;
    Values values = null;
    @Override
    public void execute(Tuple input) {
        rowkey = input.getString(0);
        header = input.getString(1);
        body = input.getString(2);

        try {
            if (body.startsWith("[")) {
                jsonObject = JSONObject.parseArray(body).getJSONObject(0);
            } else {
                jsonObject = JSONObject.parseObject(body);
            }
        } catch (Exception e) {
            //解析json异常
        }
        event = jsonObject.getString("event");
        if (null != event) {

            values = new Values(header,body);
            //根据事件发送到相关的bolt
            switch (event){
                case "$AppViewScreen" :
                    collector.emit("appViewScreen",values);
                    break;
                case "$pageview" :
                    collector.emit("pageView", values);
                    break;
                case "$WebStay":
                    collector.emit("webStay",values);
                    break;
                case "$AppStart":
                    collector.emit("appStart",values);
                    break;
                case "$AppEnd":
                    collector.emit("appEnd",values);
                    break;
                default:
                    //nothing to-do
            }
        }
    }


    @Override
    public void cleanup() {

    }


    Fields fields = new Fields("header","body");
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("appViewScreen", fields);
        declarer.declareStream("pageView", fields);
        declarer.declareStream("appStart", fields);
        declarer.declareStream("webStay",fields);
        declarer.declareStream("appEnd",fields);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
