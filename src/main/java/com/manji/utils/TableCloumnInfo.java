package com.manji.utils;

import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * Description: hbase表的列值类型信息
 * User: szw
 * Date: 2019-09-28
 * Time: 9:16
 */
public class TableCloumnInfo {
    private static HashMap<String, HashMap<String, Class>> allMap = null;

    static {
        allMap = new HashMap<>();
        HashMap<String, Class> map = null;
        //用户趋势
        map = new HashMap<>();
        map.put("dataTime", Long.class);
        map.put("startTimes", Long.class);
        map.put("newUser", Long.class);
        map.put("oldUser", Long.class);
        map.put("useTime", Long.class);

        allMap.put("user_trend", map);

        //pv ,uv 等
        map = new HashMap<>();
        map.put("dataTime",Long.class);
        map.put("viewCount",Long.class);
        map.put("useTime",Long.class);

        allMap.put("over_all_operation",map);

        //页面分析
        map = new HashMap<>();
        map.put("dataTime",Long.class);
        map.put("title",String.class);
        map.put("inPage",Long.class);
        map.put("viewCount",Long.class);
        map.put("exitPage",Long.class);

        allMap.put("page_analysis",map);

        //近期访客
        map = new HashMap<>();
        map.put("dataTime",Long.class);
        map.put("version",String.class);
        map.put("province",String.class);
        map.put("os",String.class);
        map.put("model",String.class);
        map.put("resolutionRatio",String.class);
        map.put("networkType",String.class);
        map.put("screenName",String.class);
        map.put("useTime",Long.class);
        map.put("interval",Integer.class);

        allMap.put("visitor_info",map);
    }

    /**
     * 返回字段类型，默认String.class
     * @param tableName
     * @param key
     * @return
     */
    public static Class getColumnType(String tableName, String key) {
        Class clazz = String.class;
        try {
            clazz = allMap.get(tableName).get(key);
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
        return clazz;
    }
}
