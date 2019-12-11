package com.manji.bolt.save;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.tuple.Fields;

/**
 * 保存原始数据到hbase
 */
public class SaveDataToHBaseBolt {

    public HBaseBolt SaveEventToHBase() {
        //对HBase中的表结构进行映射，有多个列簇，每个列簇有多个字段是怎么映射？
        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("rowKey")
                .withColumnFields(new Fields("header", "body"))
                .withColumnFamily("cf_infor");

        return new HBaseBolt("tb_events", mapper)
                .withConfigKey("hbConf");
    }

}
