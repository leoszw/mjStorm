package com.manji.utils;

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

public class MultiHBaseMapper implements HBaseMapper {
    private static final Logger LOG = LoggerFactory.getLogger(MultiHBaseMapper.class);
    private String rowKeyField;
    private HashMap<String, Fields> columnFamilyFields;
    private HashMap<String, Fields> counterFamilyFields;

    public MultiHBaseMapper() {
        columnFamilyFields = new HashMap<String, Fields>();
        counterFamilyFields = new HashMap<String, Fields>();
    }

    public MultiHBaseMapper withRowKeyField(String rowKeyField) {
        this.rowKeyField = rowKeyField;
        return this;
    }

    /**
     * 配置字段
     * @param columnFamily 列簇名
     * @param columnFields 字段名
     * @return
     */
    public MultiHBaseMapper withColumnFamilyFields(String columnFamily, Fields columnFields) {
        this.columnFamilyFields.put(columnFamily,columnFields);
        return this;
    }

    /**
     * 配置计数器
     * @param counterFamily 列簇名
     * @param counterFields 字段名
     * @return
     */
    public MultiHBaseMapper withCounterFamilyFields(String counterFamily, Fields counterFields) {
        this.counterFamilyFields.put(counterFamily,counterFields);
        return this;
    }

    public byte[] rowKey(Tuple tuple) {
        Object objVal = tuple.getValueByField(this.rowKeyField);
        return Utils.toBytes(objVal);
    }

    public ColumnList columns(Tuple tuple) {
        ColumnList cols = new ColumnList();
        Iterator var3;
        Fields fields;
        String field;
        Set<String> setFamily;

        if (this.columnFamilyFields != null) {
            setFamily = this.columnFamilyFields.keySet();
            for (String family : setFamily) {
                fields = this.columnFamilyFields.get(family);
                if (fields != null) {
                    var3 = fields.iterator();
                    while (var3.hasNext()) {
                        field = (String) var3.next();
                        cols.addColumn(family.getBytes(), field.getBytes(), Utils.toBytes(tuple.getValueByField(field)));
                    }
                }
            }
        }
        if (this.counterFamilyFields != null) {
            setFamily = this.counterFamilyFields.keySet();
            for (String family : setFamily) {
                fields = this.counterFamilyFields.get(family);
                if (fields != null) {
                    var3 = fields.iterator();
                    while (var3.hasNext()) {
                        field = (String) var3.next();
                        cols.addCounter(family.getBytes(), field.getBytes(), Utils.toLong(tuple.getValueByField(field)));
                    }
                }
            }
        }
        return cols;
    }
}