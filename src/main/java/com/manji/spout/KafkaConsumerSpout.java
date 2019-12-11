package com.manji.spout;

import org.apache.kafka.clients.consumer.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.time.Duration;
import java.util.*;

import static org.apache.storm.utils.Utils.tuple;

public class KafkaConsumerSpout implements IRichSpout {
    private boolean isDistributed;
    private SpoutOutputCollector collector;
    private Properties props;
    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerSpout() {
        this(true);
    }

    public KafkaConsumerSpout(boolean isDistributed) {
        this.isDistributed = isDistributed;
    }

    public boolean isDistributed() {
        return this.isDistributed;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        // 1、设置参数
        props = new Properties();
        //kafka连接信息
        props.put("bootstrap.servers", "192.168.75.128:9092");
//        props.put("bootstrap.servers", "212.64.57.234:9092");
        //        props.put("bootstrap.servers", "mjw04:9092");
        //消费者组id
        props.put("group.id", "mjw_group");
        //是否自动提交offset
        props.put("enable.auto.commit", "false");
        //在没有offset的情况下采取的拉取策略
        props.put("auto.offset.reset", "latest");
        //自动提交时间间隔
        props.put("auto.commit.interval.ms", "1000");
        //设置一次fetch请求取得的数据最大为1M
        props.put("fetch.max.bytes", "1048576");
        //key反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //value反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //重启重复消费问题
        props.put("zookeeper.connect","192.168.75.128");
//        props.put("zookeeper.connect","212.64.57.234");
        // 2、创建KafkaConsumer
        consumer = new KafkaConsumer<>(props);

        // 3、订阅数据，不给定监听器
        consumer.subscribe(Collections.singleton("mjwEvents"));
    }

    public void close() {
        try {
            if (consumer != null)   consumer.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void nextTuple() {
        Duration duration = Duration.ofSeconds(10);
        ConsumerRecords<String, String> records = consumer.poll(duration);
        try {
            for (ConsumerRecord<String, String> record : records) {
                this.collector.emit(tuple(record.value()), UUID.randomUUID());
            }
            consumer.commitSync();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Thread.yield();
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

    public void activate() {
    }

    public void deactivate() {
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}