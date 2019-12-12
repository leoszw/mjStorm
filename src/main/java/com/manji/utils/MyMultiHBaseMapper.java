package com.manji.utils;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.common.Utils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class MyMultiHBaseMapper implements HBaseMapper {
    private static final Logger LOG = LoggerFactory.getLogger(MyMultiHBaseMapper.class);
    private String rowKeyField;
    private String columnFamilyField;
    private String qualifierField;
    private String isNumberField;
    private String valueField;

    /**
     * 行键字段名称
     * @param rowKeyField
     * @return
     */
    public MyMultiHBaseMapper withRowKeyField(String rowKeyField) {
        this.rowKeyField = rowKeyField;
        return this;
    }

    /**
     * 行键字段名称
     * @param columnFamilyField
     * @return
     */
    public MyMultiHBaseMapper withColumnFamilyField(String columnFamilyField) {
        this.columnFamilyField = columnFamilyField;
        return this;
    }

    /**
     * 列簇字段名称
     * @param qualifierField
     * @return
     */
    public MyMultiHBaseMapper withQualifierField(String qualifierField) {
        this.qualifierField = qualifierField;
        return this;
    }

    /**
     * 列名字段名称
     * @param isNumberField
     * @return
     */
    public MyMultiHBaseMapper withIsNumberField(String isNumberField) {
        this.isNumberField = isNumberField;
        return this;
    }

    /**
     * 取值字段名称
     * @param valueField
     * @return
     */
    public MyMultiHBaseMapper withValueField(String valueField) {
        this.valueField = valueField;
        return this;
    }

    /**
     * 取得行键
     * @param tuple
     * @return
     */
    public byte[] rowKey(Tuple tuple) {
        Object objVal = tuple.getValueByField(this.rowKeyField);
        return Utils.toBytes(objVal);
    }

    public ColumnList columns(Tuple tuple) {
        ColumnList cols = new ColumnList();

        boolean isNum = tuple.getBooleanByField(this.isNumberField);

        if (isNum) {
            cols.addCounter(tuple.getStringByField(this.columnFamilyField).getBytes(),
                    tuple.getStringByField(this.qualifierField).getBytes(),
                    Utils.toLong(tuple.getValueByField(valueField)));
        } else {
            cols.addColumn(tuple.getStringByField(this.columnFamilyField).getBytes(),
                    tuple.getStringByField(this.qualifierField).getBytes(),
                    Utils.toBytes(tuple.getValueByField(valueField)));
        }

        return cols;
    }
}