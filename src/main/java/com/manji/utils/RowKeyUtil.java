package com.manji.utils;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * Description: rowKey生成工具类
 * User: szw
 * Date: 2019-09-21
 * Time: 17:17
 */
public class RowKeyUtil {

    //生成key
    public static List<String> getAllKeys(String[] str) {
        List<String> res = new ArrayList<String>();
        int i = 1;
        Map<String, Integer> map = new HashMap<String, Integer>();

        ArrayList<String> list = new ArrayList<String>();
        for (String s : str) {
            //过滤空key
            if (StringUtils.isNotBlank(s)) {
                map.put(s, i);
                i = i * 2;
                list.add(s);
            }
        }
        List<Integer> integers = new ArrayList<Integer>();
        listAll(list, "", 0, res, map, integers);
        return res;
    }

    public static void listAll(List<String> candidate, String prefix, int sum, List<String> res, Map<String, Integer> map, List<Integer> integers) {
        if (!prefix.equalsIgnoreCase("") && !integers.contains(sum)) {
            res.add(prefix);
            integers.add(sum);
        } //这儿体现了递归的出口
        for (int i = 0; i < candidate.size(); i++) {
            List<String> temp = new LinkedList(candidate);
            String remove = temp.remove(i);
            listAll(temp, prefix + remove, sum + map.get(remove), res, map, integers);
        }
    }

//    public static String makeRowKey(String id){
//        String md5_content = null;
//        try {
//            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
//            messageDigest.reset();
//            messageDigest.update(id.getBytes());
//            byte[] bytes = messageDigest.digest();
//            md5_content = new String(Hex.encodeHex(bytes));
//        } catch (NoSuchAlgorithmException e1) {
//            e1.printStackTrace();
//        }
//        //turn right md5
//        String right_md5_id = Integer.toHexString(Integer.parseInt(md5_content.substring(0,7),16)>>1);
//        while(right_md5_id.length()<7){
//            right_md5_id = "0" + right_md5_id;
//        }
////        return right_md5_id+"::"+id;
//        return id;
//    }
}
