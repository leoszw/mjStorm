package com.manji.spout;

import com.manji.utils.DataConstruct;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * Description: 数据源
 * User: szw
 * Date: 2019-09-27
 * Time: 14:56
 */
public class MJSpout implements IRichSpout {
    SpoutOutputCollector collector;

    Random random;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        random = new Random();
    }

    public void nextTuple() {
        try {
            Thread.sleep(2000);
//            collector.emit(new Values(DataConstruct.getData(0)));  //pageView
//            collector.emit(new Values(DataConstruct.getData(1)));  //webStay
//            collector.emit(new Values(DataConstruct.getData(2)));  //pageView
//            collector.emit(new Values(DataConstruct.getData(3)));  //pageView
            collector.emit(new Values(DataConstruct.getData(4))); //appStart
            collector.emit(new Values(DataConstruct.getData(5))); //append
            collector.emit(new Values(DataConstruct.getData(6))); //register
            collector.emit(new Values(DataConstruct.getData(7))); //UpdateApp
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }
}
