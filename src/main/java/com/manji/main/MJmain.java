package com.manji.main;

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
import org.apache.storm.tuple.Fields;

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
        //数据源
//        topologyBuilder.setSpout("mjSpout", new MJSpout(), 1);
        topologyBuilder.setSpout("mjSpout_kafka",new KafkaConsumerSpout(),1);

        //分割数据
        topologyBuilder.setBolt("mjSpout_split", new SplitOriginalDataBolt(), 1).shuffleGrouping("mjSpout_kafka");
        //保存原始数据，猜测storm-core2.0的包才能用，开发使用1.0
        topologyBuilder.setBolt("old_data", new SaveDataToHBaseBolt().SaveEventToHBase(), 1).
                fieldsGrouping("mjSpout_split", new Fields("rowKey", "header", "body"));

        //数据分发
        topologyBuilder.setBolt("mjSpout",new SpoutFilterBolt()).shuffleGrouping("mjSpout_split");

        //解析app页面浏览数据
        topologyBuilder.setBolt("parseAppViewScreen",new ParseAppViewScreenBolt()).shuffleGrouping("mjSpout","appViewScreen");
        //APP页面访问量统计
        topologyBuilder.setBolt("PV_APP",new SavePVBolt("over_all_operation")).shuffleGrouping("parseAppViewScreen");
        //APP访问用户数
        topologyBuilder.setBolt("UV_APP",new SaveUVBolt("over_all_operation")).shuffleGrouping("parseAppViewScreen");
        //统计在线访客
        topologyBuilder.setBolt("visitor_view",new SaveVisitorInfoBolt("visitor_info")).shuffleGrouping("parseAppViewScreen");
        //统计页面分析
        topologyBuilder.setBolt("pageAnalysis_view",new SavePageAnalysisBolt("page_analysis")).shuffleGrouping("parseAppViewScreen");

        //wap pc 解析页面浏览数据
        topologyBuilder.setBolt("parsePageView", new ParsePageViewLogBolt()).shuffleGrouping("mjSpout","pageView");
        //wap pc 页面访问量统计
        topologyBuilder.setBolt("PV_WAPORPC",new SavePVBolt("over_all_operation")).shuffleGrouping("parsePageView");
        //wap pc 访问用户数
        topologyBuilder.setBolt("UV_WAPORPC",new SaveUVBolt("over_all_operation")).shuffleGrouping("parsePageView");


        //WebStay事件解析
        topologyBuilder.setBolt("parseWebStay",new ParseWebStayLogBolt()).shuffleGrouping("mjSpout","webStay");
        //统计使用时长--WAP PC
        topologyBuilder.setBolt("useTime_webStay",new SaveUseTimeBolt("over_all_operation")).shuffleGrouping("parseWebStay");

        //AppEnd事件解析
        topologyBuilder.setBolt("parseAppEnd",new ParseAppEndLogBolt()).shuffleGrouping("mjSpout","appEnd");
        //统计使用时长--APP + PC + WAP
        topologyBuilder.setBolt("useTime_appEnd",new SaveUseTimeBolt("over_all_operation"),3).shuffleGrouping("parseAppEnd");
        //统计App使用时长
        topologyBuilder.setBolt("useTime_onlyApp",new SaveUseTimeBolt("user_trend",true,true),3).shuffleGrouping("parseAppEnd");
        //统计在线访客
        topologyBuilder.setBolt("visitor_end",new SaveVisitorInfoBolt("visitor_info")).shuffleGrouping("parseAppEnd");
        //统计页面分析
        topologyBuilder.setBolt("pageAnalysis_end",new SavePageAnalysisBolt("page_analysis"),3).shuffleGrouping("parseAppEnd");

        //AppStart事件解析
        topologyBuilder.setBolt("parseAppStart",new ParseAppStartLogBolt()).shuffleGrouping("mjSpout","appStart");
        //统计启动次数
        topologyBuilder.setBolt("start_times",new SaveStartTimesBolt("user_trend"),3).shuffleGrouping("parseAppStart");
        //统计启动用户数
        topologyBuilder.setBolt("start_users",new SaveStartUserBolt("user_trend"),3).shuffleGrouping("parseAppStart");
        //统计在线访客
        topologyBuilder.setBolt("visitor_start",new SaveVisitorInfoBolt("visitor_info")).shuffleGrouping("parseAppStart");
        //统计页面分析
        topologyBuilder.setBolt("pageAnalysis_start",new SavePageAnalysisBolt("page_analysis"),3).shuffleGrouping("parseAppStart");






//        String[] pageViewKeys = {"appName", "year", "month", "week", "times", "version", "userGroup", "urlPath", "referrerPath"};
//        //wap pc 统计页面浏览数据
//        topologyBuilder.setBolt("pageView", new PageViewBolt(pageViewKeys, "t1")).shuffleGrouping("parsePageView");
//
//
//        String[] pageStayKeys = {"appName", "year", "month", "week", "times", "version", "userGroup", "urlPath"};
//        //页面停留事件数据
//        topologyBuilder.setBolt("webStayFilter", new SpoutFilterBolt()).shuffleGrouping("mjSpout","webStay");
//        //解析页面停留数据
//        topologyBuilder.setBolt("parseWebStay", new ParseWebStayLogBolt()).shuffleGrouping("webStayFilter");
//        //统计页面停留数据
//        topologyBuilder.setBolt("webStay", new WebStayBolt(pageStayKeys, "t1")).shuffleGrouping("parseWebStay");
//
////        String[] appStartkeys = {"appName","year","month","week","times","version","province","userGroup","userID"};
//        String[] appStartkeys = {"appName", "year", "month", "week", "times", "version", "province", "userGroup", "userID"};
//        //app启动事件数据
//        topologyBuilder.setBolt("appStartFilter", new SpoutFilterBolt()).shuffleGrouping("mjSpout","appStart");
//        //解析app启动事件数据
//        topologyBuilder.setBolt("parseAppStart", new ParseAppStartLogBolt()).shuffleGrouping("appStartFilter");
//        //统计app启动数据(地域分布，版本分布，终端分布)
//        topologyBuilder.setBolt("appStart", new AppStartBolt(appStartkeys, "t2")).shuffleGrouping("parseAppStart");
//
//        String[] appEndForAreaKeys = {"appName", "year", "month", "week", "times", "version", "province", "userID"};
//        String[] appEndForPageViewKeys = {"appName", "year", "month", "week", "times", "version", "userGroup", "urlPath"};
//        //app退出事件数据
//        topologyBuilder.setBolt("appEndFilter", new SpoutFilterBolt()).shuffleGrouping("mjSpout","appEnd");
//        //解析app退出事件数据
//        topologyBuilder.setBolt("parseAppEnd", new ParseAppEndLogBolt()).shuffleGrouping("appEndFilter");
//        //统计app使用时间
//        topologyBuilder.setBolt("appEndForArea", new AppEndForAreaDistributBolt(appEndForAreaKeys, "t2")).shuffleGrouping("parseAppEnd");
//        //统计页面退出率
//        topologyBuilder.setBolt("appEndForPageView", new AppEndForPageViewBolt(appEndForPageViewKeys, "t1")).shuffleGrouping("parseAppEnd");

//        String[] registerKeys = {"appName", "year", "month", "week", "times", "version", "userGroup", "province", "userID"};
//        //注册事件数据
////        topologyBuilder.setBolt("registerFilter", new SpoutFilterBolt()).shuffleGrouping("mjSpout");
//        //解析注册事件数据
//        topologyBuilder.setBolt("parseRegister", new ParseRegisterLogBolt()).shuffleGrouping("registerFilter");
//        //统计注册事件数据
//        topologyBuilder.setBolt("register", new RegisterBolt(registerKeys, "t2")).shuffleGrouping("parseRegister");
//
//        String[] updateAppKeys = {"appName", "year", "month", "week", "times", "version", "userGroup", "province", "userID"};
//        //app升级事件数据
////        topologyBuilder.setBolt("updateAppFilter", new SpoutFilterBolt()).shuffleGrouping("mjSpout");
//        //解释app升级事件数据
//        topologyBuilder.setBolt("parseUpdateApp", new ParseUpdateAppLogBolt()).shuffleGrouping("updateAppFilter");
//        //统计app升级事件数据
//        topologyBuilder.setBolt("updateApp", new UpdateAppBolt(updateAppKeys, "t2")).shuffleGrouping("parseUpdateApp");

//        /**
//         * 用户趋势统计数据
//         */
//        String[] userTrendKeys = {"appName", "year", "month", "week", "times", "version", "userGroup", "userID"};
//        //统计用户趋势新用户数、老用户数、启动用户数、启动次数
//        topologyBuilder.setBolt("userTrendAppStart", new UserTrendAppStartBolt(userTrendKeys, "t3")).shuffleGrouping("parseAppStart");
//        //统计用户趋势使用时长
//        topologyBuilder.setBolt("userTrendAppEnd", new UserTrendAppEndBolt(userTrendKeys, "t3")).shuffleGrouping("parseAppEnd");
//
//        /**
//         * 用户活跃度统计
//         */
//        String[] activeUserKeys = {"appName", "year", "month", "week", "times", "version", "userGroup", "userID"};
//        //统计登录明细
//        topologyBuilder.setBolt("activeUserAppStart",new ActiveUserAppStartBolt(activeUserKeys,"t4")).shuffleGrouping("parseAppStart");

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
