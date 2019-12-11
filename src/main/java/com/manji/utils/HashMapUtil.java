package com.manji.utils;

import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: szw
 * Date: 2019-09-20
 * Time: 9:03
 */
public class HashMapUtil {

    public static String getStrFromHashMap(String key, HashMap<String, Object> hashMap) {
        if (hashMap == null) return null;
        return hashMap.get(key) == null ? null : hashMap.get(key).toString();
    }

    public static <T> T getValFromHashMap(String key, HashMap<String, Object> hashMap, Class<T> t) {
        if (hashMap == null) return null;
        Object object = hashMap.get(key);
        if (object == null) return null;
        return (T) object;
    }

    public static Double getDoubleFromHashMap(String key, HashMap<String, Object> hashMap) {
        if (hashMap == null) return null;
        return hashMap.get(key) == null ? null : Double.valueOf(hashMap.get(key).toString());
    }

    public static void putCount(String key, HashMap<String, Long> hashMap) {
        hashMap.put(key, (hashMap.get(key) == null ? 0L : hashMap.get(key)) + 1);
    }
}
