package com.manji.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: szw
 * Date: 2019-09-20
 * Time: 10:39
 */
@SuppressWarnings("unchecked")
public class HbaseUtil {
    /**
     * 获取hbase连接
     *
     * @return
     */
    private static ExecutorService executor = null;
    private static Connection conn = null;

    /**
     * 创建HBase连接
     */
    public static void createConn(){
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.75.128:2181");
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            executor = Executors.newFixedThreadPool(20);
            conn = ConnectionFactory.createConnection(conf, executor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭连接
     *
     * @param conn
     */
    public static void closeConn(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取列的值
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param cloumnName
     * @return
     * @throws Exception
     */
    public static <T> T getCellValue(String tableName, String rowKey, String familyName, String cloumnName) throws Exception {
        if(null == conn) createConn();
        if (StringUtils.isBlank(rowKey)) return null;
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(familyName.getBytes(), cloumnName.getBytes());
        Result result = table.get(get);
        byte[] resByte = null;
        if (result != null) {
            resByte = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(cloumnName));
        }
        if (resByte == null) return null;

        Class clazz = TableCloumnInfo.getColumnType(tableName, cloumnName);

        if (clazz != null) {
            if (clazz.equals(Long.class)) {
                return (T) (Object) Bytes.toLong(resByte);
            } else if (clazz.equals(Double.class)) {
                return (T) (Object) Bytes.toDouble(resByte);
            } else if (clazz.equals(Float.class)) {
                return (T) (Object) Bytes.toFloat(resByte);
            } else if (clazz.equals(Short.class)) {
                return (T) (Object) Bytes.toShort(resByte);
            } else if (clazz.equals(Integer.class)) {
                return (T) (Object) Bytes.toInt(resByte);
            } else if (clazz.equals(BigDecimal.class)) {
                return (T) Bytes.toBigDecimal(resByte);
            } else if (clazz.equals(Boolean.class)) {
                return (T) (Object) Bytes.toBoolean(resByte);
            } else {
                return (T) Bytes.toString(resByte);
            }
        }
        return null;
    }

    /**
     * 插入一条数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRow(String tableName, String rowKey, String columnFamily, String column, Object value) throws IOException {
        if(null == conn) createConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));  //   通过rowkey创建一个 put 对象
        //  在 put 对象中设置 列族、列、值
        Class clazz = TableCloumnInfo.getColumnType(tableName, column);
        if (clazz != null) {
            if (clazz.equals(Long.class)) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(Long.valueOf(value + "")));
            } else if (clazz.equals(Double.class)) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(Double.valueOf(value + "")));
            } else if (clazz.equals(Float.class)) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(Float.valueOf(value + "")));
            } else if (clazz.equals(Short.class)) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(Short.valueOf(value + "")));
            } else if (clazz.equals(Integer.class)) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(Integer.valueOf(value + "")));
            } else if (clazz.equals(BigDecimal.class)) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(new BigDecimal(value + "")));
            } else if (clazz.equals(Boolean.class)) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(Boolean.valueOf(value + "")));
            } else {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value + ""));
            }
        }
        table.put(put);     //  插入数据，可通过 put(List<Put>) 批量插入
        table.close();
    }

    /**
     * 获取一条数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static JSONObject getRow(String tableName, String rowKey, String[] column, String[] family) throws IOException {

        if(null == conn) createConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));   //  通过rowkey创建一个 get 对象
        Result result = table.get(get);         //  输出结果

        JSONObject jsonObject = null;
        Class clazz = null;
        byte[] bytes = null;
        for (Cell cell : result.rawCells()) {
            jsonObject = new JSONObject();
            jsonObject.put("rowkey", Bytes.toString(result.getRow()));
            if (column != null && family != null) {
                for (int i = 0; i < column.length; i++) {
                    clazz = TableCloumnInfo.getColumnType(tableName, column[i]);
                    bytes = result.getValue(family[i].getBytes(), column[i].getBytes());
                    if (clazz != null && bytes != null) {
                        if (clazz.equals(Long.class)) {
                            jsonObject.put(column[i], Bytes.toLong(bytes));
                        } else if (clazz.equals(Double.class)) {
                            jsonObject.put(column[i], Bytes.toDouble(bytes));
                        } else if (clazz.equals(Float.class)) {
                            jsonObject.put(column[i], Bytes.toFloat(bytes));
                        } else if (clazz.equals(Short.class)) {
                            jsonObject.put(column[i], Bytes.toShort(bytes));
                        } else if (clazz.equals(Integer.class)) {
                            jsonObject.put(column[i], Bytes.toInt(bytes));
                        } else if (clazz.equals(BigDecimal.class)) {
                            jsonObject.put(column[i], Bytes.toBigDecimal(bytes));
                        } else if (clazz.equals(Boolean.class)) {
                            jsonObject.put(column[i], Bytes.toBoolean(bytes));
                        } else {
                            jsonObject.put(column[i], Bytes.toString(bytes));
                        }
                    } else {
                        jsonObject.put(column[i], null);
                    }
                }
            }
        }
        table.close();

        return jsonObject;
    }

    /**
     * 全表扫描
     *
     * @param tableName
     * @throws IOException
     */
    public static List<JSONObject> scanTable(String tableName, FilterList filterList, String[] column, String[] family) throws IOException {

        if(null == conn) createConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();  //  创建扫描对象
        if (filterList != null) scan.setFilter(filterList);//设置过滤器
        ResultScanner results = table.getScanner(scan);   //  全表的输出结果
        List<JSONObject> list = new ArrayList<>();
        JSONObject jsonObject = null;
        Class clazz = null;
        byte[] bytes = null;

        for (Result result : results) {
            jsonObject = new JSONObject();
            jsonObject.put("rowkey", Bytes.toString(result.getRow()));
            for (int i = 0; i < column.length; i++) {
                clazz = TableCloumnInfo.getColumnType(tableName, column[i]);
                bytes = result.getValue(family[i].getBytes(), column[i].getBytes());
                if (clazz != null && bytes != null) {
                    if (clazz.equals(Long.class)) {
                        jsonObject.put(column[i], Bytes.toLong(bytes));
                    } else if (clazz.equals(Double.class)) {
                        jsonObject.put(column[i], Bytes.toDouble(bytes));
                    } else if (clazz.equals(Float.class)) {
                        jsonObject.put(column[i], Bytes.toFloat(bytes));
                    } else if (clazz.equals(Short.class)) {
                        jsonObject.put(column[i], Bytes.toShort(bytes));
                    } else if (clazz.equals(Integer.class)) {
                        jsonObject.put(column[i], Bytes.toInt(bytes));
                    } else if (clazz.equals(BigDecimal.class)) {
                        jsonObject.put(column[i], Bytes.toBigDecimal(bytes));
                    } else if (clazz.equals(Boolean.class)) {
                        jsonObject.put(column[i], Bytes.toBoolean(bytes));
                    } else {
                        jsonObject.put(column[i], Bytes.toString(bytes));
                    }
                } else {
                    jsonObject.put(column[i], null);
                }
            }
            list.add(jsonObject);
        }
        results.close();
        table.close();
        return list;
    }

    /**
     * 删除一条数据
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void delRow(String tableName, String rowKey) throws IOException {

        if(null == conn) createConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
        conn.close();
    }

    /**
     * 删除多条数据
     *
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void delRows(String tableName, String[] rows) throws IOException {

        if(null == conn) createConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        List<Delete> list = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            list.add(delete);
        }
        table.delete(list);
        table.close();
        conn.close();
    }

    /**
     * 删除列族
     *
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void delColumnFamily(String tableName, String columnFamily)
            throws IOException {

        if(null == conn) createConn();
        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();  // 创建数据库管理员
        hAdmin.deleteColumn(TableName.valueOf(tableName), columnFamily.getBytes());
        conn.close();
    }

    /**
     * 删除数据库表
     *
     * @param table_Name
     * @throws IOException
     */
    public static void deleteTable(String table_Name) throws IOException {
        if(null == conn) createConn();
        TableName tableName = TableName.valueOf(table_Name);
        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();   // 创建数据库管理员
        if (hAdmin.tableExists(tableName)) {
            hAdmin.disableTable(tableName);     //  失效表
            hAdmin.deleteTable(tableName);     //  删除表
            System.out.println("删除" + tableName + "表成功");
            conn.close();
        } else {
            System.out.println("需要删除的" + tableName + "表不存在");
            conn.close();
            System.exit(0);
        }
    }

    /**
     * 追加插入
     * 在原有的value后面追加新的value，  "a" + "bc"  -->  "abc"
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void appendData(String tableName, String rowKey, String columnFamily,
                                  String column, String value) throws IOException {

        if(null == conn) createConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Append append = new Append(Bytes.toBytes(rowKey));
        append.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.append(append);
        table.close();
        conn.close();
    }

    /**
     * 符合条件后添加数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamilyCheck
     * @param columnCheck
     * @param valueCheck
     * @param columnFamily
     * @param column
     * @param value
     * @return
     * @throws IOException
     */
    public static boolean checkAndPut(String tableName, String rowKey,
                                      String columnFamilyCheck, String columnCheck, String valueCheck,
                                      String columnFamily, String column, String value) throws IOException {

        if(null == conn) createConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        boolean result = table.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamilyCheck),
                Bytes.toBytes(columnCheck), Bytes.toBytes(valueCheck), put);
        table.close();
        conn.close();
        return result;
    }

    /**
     * 符合条件后删除数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamilyCheck
     * @param columnCheck
     * @param valueCheck
     * @param columnFamily
     * @param column
     * @return
     * @throws IOException
     */
    public static boolean checkAndDelete(String tableName, String rowKey,
                                         String columnFamilyCheck, String columnCheck, String valueCheck,
                                         String columnFamily, String column) throws IOException {

        if(null == conn) createConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(columnFamilyCheck), Bytes.toBytes(columnCheck));
        boolean result = table.checkAndDelete(Bytes.toBytes(rowKey),
                Bytes.toBytes(columnFamilyCheck), Bytes.toBytes(columnCheck),
                Bytes.toBytes(valueCheck), delete);
        table.close();
        conn.close();
        return result;
    }

    /**
     * 计数器
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param amount
     * @return
     * @throws IOException
     */
    public static long incrementColumnValue(String tableName, String rowKey,
                                            String columnFamily, String column, long amount) throws IOException {

        if(null == conn) createConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        long result = table.incrementColumnValue(Bytes.toBytes(rowKey),
                Bytes.toBytes(columnFamily), Bytes.toBytes(column), amount);
        table.close();
        return result;
    }

    /**
     * 判断表是否存在
     *
     * @param table_name
     * @return
     * @throws IOException
     */
    public static boolean tableExists(String table_name) throws IOException {
        if(null == conn) createConn();
        TableName tableName = TableName.valueOf(table_name);
        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();
        return hAdmin.tableExists(tableName);
    }

    /**
     * 判断列族是否存在
     *
     * @param table_name
     * @param familyName
     * @return
     * @throws IOException
     */
    public static boolean familyExists(String table_name, String familyName) throws IOException {
        if(null == conn) createConn();
        TableName tableName = TableName.valueOf(table_name);
        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();
        if (hAdmin.tableExists(TableName.valueOf(table_name))) {
            Table table = conn.getTable(tableName);
            HTableDescriptor tableDescriptor = table.getTableDescriptor();
            HColumnDescriptor descriptor = tableDescriptor.getFamily(Bytes.toBytes(familyName));
            return descriptor != null;
        }
        return false;
    }

    /**
     * 全表查询，根据行键，列匹配
     *
     * @param table_name
     * @param rowkeyReg
     * @param familyName
     * @param columnName
     * @param colValue
     * @return
     * @throws IOException
     */
    public static ResultScanner filterKey(String table_name, String rowkeyReg, String familyName, String columnName, String colValue) throws IOException {
        if(null == conn) createConn();
        Table table = conn.getTable(TableName.valueOf(table_name));
        Scan scan = new Scan();
        // 构建模糊查询的Filter和分页的Filter
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        if (rowkeyReg != null) {
            RegexStringComparator regex = new RegexStringComparator(rowkeyReg);
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, regex);
            filterList.addFilter(filter);
        }
        if (StringUtils.isNotBlank(familyName) && StringUtils.isNotBlank(columnName)) {
            filterList.addFilter(new SingleColumnValueFilter(familyName.getBytes(), columnName.getBytes(), CompareFilter.CompareOp.EQUAL, colValue.getBytes()));
        }

        scan.setFilter(filterList);
        ResultScanner rs = table.getScanner(scan);
        return rs;
    }

    /**
     * 批量修改数据
     *
     * @param table_name
     * @param res
     * @param family
     * @param column
     * @param value
     * @param t
     */
    public static void batchEdit(String table_name, ResultScanner res, String family, String column, Object value, Class t) {
        if (res != null && value != null) {
            Result result;
            try {
                while ((result = res.next()) != null) {
                    String rowkey = Bytes.toString(result.getRow());
                    if (t.equals(Long.class)) {
                        incrementColumnValue(table_name, rowkey, family, column, Long.parseLong(value + ""));
                    } else {
                        addRow(table_name, rowkey, family, column, value.toString());
                    }
                }
            } catch (IOException e) {

            }
        }
    }

    /**
     * 批量生成新数据
     *
     * @param table_name
     * @param res
     * @param family
     * @param column
     * @param value
     * @param t
     */
    public static void batchAdd(String table_name, ResultScanner res, String family, String column, Object value, Class t) {
        if (res != null && value != null) {
            Result result;
            try {
                while ((result = res.next()) != null) {
                    String rowkey = Bytes.toString(result.getRow()) + "NO" + (new Date().getTime());
                    if (t.equals(Long.class)) {
                        incrementColumnValue(table_name, rowkey, family, column, Long.parseLong(value + ""));
                    } else {
                        addRow(table_name, rowkey, family, column, value.toString());
                    }
                }
            } catch (IOException e) {

            }
        }
    }

    /**
     * 查询上一次启动时间
     * @param tableName
     * @param rowKeyReg
     * @param column
     * @param family
     * @return
     */
    public static Long getBeforStartTime(String tableName,String rowKeyReg,String family, String column){
        if(null == conn) createConn();
        try {
            Scan scan = new Scan();
            scan.setReversed(true);
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(rowKeyReg)));
            filterList.addFilter(new PageFilter(1));
            scan.setFilter(filterList);

            Table table = conn.getTable(TableName.valueOf(tableName));
            ResultScanner scanner = table.getScanner(scan);
            for(Result result : scanner){
                byte[] bytes = result.getValue(family.getBytes(), column.getBytes());
                if(null != bytes){
                    return Bytes.toLong(bytes);
                }
                break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}

