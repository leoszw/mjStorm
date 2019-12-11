package com.manji.utils;

import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: szw
 * Date: 2019-09-19
 * Time: 17:16
 */
public class ParseLogUtil {

    public static HashMap<String, Object> parseLog(String header,String body) {
        if (header == null || body == null) return null;
        JSONObject json_body = null;
        JSONObject json_header = null;
        HashMap<String, Object> hashMap = new HashMap<String, Object>();
        try {
            if (body.startsWith("[")) {
                json_body = JSONObject.parseArray(body).getJSONObject(0);
                json_header = JSONObject.parseArray(header).getJSONObject(0);
            } else {
                json_body = JSONObject.parseObject(body);
                json_header = JSONObject.parseObject(header);
            }
        } catch (Exception e) {
            //解析json异常
        }


        //根据事件，获取不同的属性
        String event = json_body.getString("event");
        hashMap.put("event", event);//事件名称

        if (event != null) {
            //公共属性
            hashMap.put("timestamp", json_body.getString("time")); //数据时间

            hashMap.put("distinctId", json_body.getString("distinct_id"));//用户标识

            JSONObject properties = json_body.getJSONObject("properties");
            JSONObject lib = json_body.getJSONObject("lib");

            switch (event) {
                case "$AppViewScreen": //app页面浏览事件
                    hashMap.put("screenName",properties.getString("$screen_name"));    //当前页面
                    hashMap.put("appName",properties.getString("app_name"));           //app名称
                    hashMap.put("os", properties.getString("$os") + properties.getString("$os_version")); //操作系统
                    hashMap.put("title",properties.getString("$title"));               //页面名称
                    hashMap.put("userGroup",properties.getString("userGroup"));        //用户分组
                    break;
                case "$pageview"://wap 和 pc端页面浏览事件
                    hashMap.put("referrerPath", properties.getString("$referrer"));      //上页路径
                    hashMap.put("url", properties.getString("$url"));                    //当前页路径
                    hashMap.put("title", properties.getString("$title"));                //当前页名称
                    hashMap.put("urlPath", properties.getString("$url_path"));           //当前页路径
                    hashMap.put("appName", properties.getString("app_name"));            //应用名称
                    hashMap.put("referrerName", json_body.getString("referrerName"));    //上页名称
                    hashMap.put("version", properties.getString("version"));             //应用版本
                    hashMap.put("userGroup", properties.getString("userGroup"));         //用户分组
                    hashMap.put("userAgent",json_header.getString("user-agent"));        //userAgent，用于判断 wap 还是 pc 访问
                    break;
                case "$WebStay"://页面停留事件
                    hashMap.put("appName", properties.getString("app_name"));            //应用名称
                    hashMap.put("urlPath", properties.getString("$url_path"));           //停留页路径
                    hashMap.put("version", properties.getString("$app_version"));        //app版本
                    hashMap.put("userGroup", properties.getString("userGroup"));         //用户分组
                    hashMap.put("pageStay", properties.getString("$event_duration"));    //停留时间
                    break;
                case "$WebClick":

                    break;
                case "$AppStart"://app开始事件
                    hashMap.put("version", properties.getString("$app_version"));        //app版本
                    hashMap.put("appName", properties.getString("app_name"));            //应用名称
                    hashMap.put("province", properties.getString("province"));           //省份
                    hashMap.put("userID", properties.getString("userID"));               //用户ID
                    hashMap.put("equipment", properties.getString("equipment"));         //设备号
                    hashMap.put("userGroup", properties.getString("user_group"));        //用户组

                    hashMap.put("brand", properties.getString("$manufacturer"));         //设备生产商（品牌）
                    hashMap.put("model", properties.getString("$model"));                //设备型号
                    hashMap.put("networkType", properties.getString("$network_type"));   //联网方式
                    hashMap.put("os", properties.getString("$os") + properties.getString("$os_version"));                               //操作系统
                    hashMap.put("resolutionRatio", properties.getString("$screen_height") + "*" + properties.getString("$screen_width"));     //屏幕分辨率
                    hashMap.put("regsiterTime",properties.getString("register_time"));    //用户注册时间
                    hashMap.put("screenName",properties.getString("$screen_name"));       //当前页面
                    hashMap.put("title",properties.getString("$title"));                  //页面名称
                    break;
                case "$AppEnd"://app结束事件
                    hashMap.put("version", properties.getString("$app_version"));        //app版本
                    hashMap.put("appName", properties.getString("app_name"));            //应用名称
                    hashMap.put("province", properties.getString("province"));           //省份
                    hashMap.put("userID", properties.getString("userID"));               //用户ID
                    hashMap.put("equipment", properties.getString("equipment"));         //设备号
                    hashMap.put("useTime", properties.getString("event_duration"));      //使用时长
                    hashMap.put("screenName", properties.getString("$screen_name"));     //退出页地址
                    hashMap.put("userGroup", properties.getString("user_group"));        //用户组

                    hashMap.put("brand", properties.getString("$manufacturer"));         //设备生产商（品牌）
                    hashMap.put("model", properties.getString("$model"));                 //设备型号
                    hashMap.put("networkType", properties.getString("$network_type"));   //联网方式
                    hashMap.put("os", properties.getString("$os") + properties.getString("$os_version"));                               //操作系统
                    hashMap.put("resolutionRatio", properties.getString("$screen_height") + "*" + properties.getString("$screen_width"));     //屏幕分辨率
                    hashMap.put("title",properties.getString("$title"));               //页面名称
                    break;
                case "$Register"://注册事件
                    hashMap.put("version", properties.getString("$app_version"));        //app版本
                    hashMap.put("appName", properties.getString("app_name"));            //应用名称
                    hashMap.put("province", properties.getString("province"));           //省份
                    hashMap.put("userID", properties.getString("userID"));               //用户ID
                    hashMap.put("equipment", properties.getString("equipment"));         //设备号
                    hashMap.put("userGroup", null);                                            //用户组

                    hashMap.put("brand", properties.getString("$manufacturer"));         //设备生产商（品牌）
                    hashMap.put("model", properties.getString("$model"));                 //设备型号
                    hashMap.put("networkType", properties.getString("$network_type"));   //联网方式
                    hashMap.put("os", properties.getString("$os") + properties.getString("$os_version"));                               //操作系统
                    hashMap.put("resolutionRatio", properties.getString("$screen_height") + "*" + properties.getString("$screen_width"));     //屏幕分辨率
                    break;
                case "$UpdateApp"://升级APP事件
                    hashMap.put("version", properties.getString("$app_version_up"));     //升级到的app版本
                    hashMap.put("appName", properties.getString("app_name"));            //应用名称
                    hashMap.put("province", properties.getString("province"));           //省份
                    hashMap.put("userID", properties.getString("userID"));               //用户ID
                    hashMap.put("equipment", properties.getString("equipment"));         //设备号
                    hashMap.put("userGroup", properties.getString("userGroup"));         //用户组
                    break;
            }
        }

        return hashMap;
    }

}
