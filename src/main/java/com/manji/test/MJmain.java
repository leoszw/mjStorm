package com.manji.test;

import com.manji.bolt.filter.SpoutFilterBolt;
import com.manji.bolt.other.SplitOriginalDataBolt;
import com.manji.bolt.parse.*;
import com.manji.bolt.save.*;
import com.manji.spout.KafkaConsumerSpout;
import com.manji.utils.HbaseUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * Description: 主类
 * User: szw
 * Date: 2019-09-27
 * Time: 16:47
 */
public class MJmain {

    public static void main(String[] args) {
        HbaseUtil.createConn();
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        SaveDataToHBaseBolt dataBolt = new SaveDataToHBaseBolt();
        //数据源
//        topologyBuilder.setSpout("mjSpout", new MJSpout(), 1);
        topologyBuilder.setSpout("mjSpout_kafka",new KafkaConsumerSpout(),1);

        //分割数据
        topologyBuilder.setBolt("mjSpout_split", new SplitOriginalDataBolt(), 1).shuffleGrouping("mjSpout_kafka");
//        //保存原始数据，猜测storm-core2.0的包才能用，开发使用1.0
//        topologyBuilder.setBolt("old_data", new SaveDataToHBaseBolt().SaveEventToHBase(), 1).
//                fieldsGrouping("mjSpout_split", new Fields("rowKey", "header", "body"));

        //数据分发
        topologyBuilder.setBolt("mjSpout",new SpoutFilterBolt()).shuffleGrouping("mjSpout_split");

        //解析app页面浏览数据
        topologyBuilder.setBolt("parseAppViewScreen",new ParseAppViewScreenBolt()).shuffleGrouping("mjSpout","appViewScreen");

        //维度分析
        topologyBuilder.setBolt("aaa",new PVTestBolt()).shuffleGrouping("parseAppViewScreen");
        //APP页面访问量统计
        topologyBuilder.setBolt("PV_APP",dataBolt.SaveToHBase("t1")).shuffleGrouping("aaa");



        StormTopology topology = topologyBuilder.createTopology();

        // 5. 创建配置对象
        Config conf = new Config();
        conf.setNumWorkers(2);

        Map<String, Object> hbConf = new HashMap<String, Object>();
        hbConf.put("hbase.zookeeper.quorum", "192.168.75.128:2181");
        conf.put("hbConf", hbConf);

        // 6. 提交Topology给集群运行
        if (args.length == 0 || args[0] == null) {
            LocalCluster cluster = null;
            try {
                cluster = new LocalCluster();
                cluster.submitTopology("eventAnalysis", conf, topology);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            try {
                StormSubmitter.submitTopology(args[0], conf, topology);
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }
    }
}
