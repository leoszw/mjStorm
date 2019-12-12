package com.manji.bolt.save;

import com.manji.utils.MultiHBaseMapper;
import com.manji.utils.MyMultiHBaseMapper;
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

    /**
     * 保存数据到hbase
     * @param tableName 表名
     * @return
     */
    public HBaseBolt SaveToHBase(String tableName){
        MyMultiHBaseMapper mapper = new MyMultiHBaseMapper()
                .withRowKeyField("rowKey")
                .withColumnFamilyField("cf")
                .withQualifierField("cq")
                .withValueField("value")
                .withIsNumberField("isNum");

        return new HBaseBolt(tableName, mapper)
                .withConfigKey("hbConf");
    }

}
