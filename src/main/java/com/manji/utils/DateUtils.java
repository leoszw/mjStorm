package com.manji.utils;

import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: szw
 * Date: 2019-09-18
 * Time: 11:27
 */
public class DateUtils {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private static final SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy/mm/dd");

    //解析获取时间
    public static String parseTime(Object obj) {
        String result = null;
        if (null != obj) {
            try {
                Long times = Long.parseLong(obj.toString());
                result = sdf.format(new Date(times));
                return result;
            } catch (Exception e) {
                //不是long类型
            }
            try {
                if (obj.getClass().equals(Date.class)) {
                    return sdf.format(obj);
                } else {
                    if (obj.toString().indexOf("/") != -1 && obj.toString().length() > 16) {
                        return sdf.format(sdf1.parse(obj.toString()));
                    } else if (obj.toString().indexOf("/") != -1) {
                        return sdf.format(sdf3.parse(obj.toString()));
                    } else if (obj.toString().indexOf("-") != -1 && obj.toString().length() > 16) {
                        return sdf.format(sdf2.parse(obj.toString()));
                    } else if (obj.toString().indexOf("-") != -1) {
                        return obj.toString();
                    }
                }
            } catch (Exception e) {
                //解析错误
                e.printStackTrace();
            }
        }
        return result;

    }

    //获取周
    public static String getWeek(String time) {
        try {
            Date date = new Date(Long.parseLong(time));
            Calendar calendar = Calendar.getInstance();
            calendar.setFirstDayOfWeek(Calendar.MONDAY);
            calendar.setTime(date);
            return calendar.get(Calendar.WEEK_OF_YEAR) + "";
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //获取月
    public static String getMonth(String time) {
        try {
            return sdf.format(new Date(Long.parseLong(time))).split("-")[1];
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //获取年
    //获取月
    public static String getYear(String time) {
        try {
            return sdf.format(new Date(Long.parseLong(time))).split("-")[0];
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取时间毫秒
     *
     * @param time
     * @return
     */
    public static Long getTimeMills(String time) {
        try {
            return sdf.parse(parseTime(time)).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取小时
     * @param obj
     * @return
     */
    public static Integer getHour(Object obj){
        if (null != obj) {
            try {
                Long times = Long.parseLong(obj.toString());
                return new Date(times).getHours();
            } catch (Exception e) {
                //不是long类型
            }
            try {
                if (obj.getClass().equals(Date.class)) {
                    return ((Date)obj).getHours();
                } else {
                    if (obj.toString().indexOf("/") != -1 && obj.toString().length() > 16) {
                        return sdf1.parse(obj.toString()).getHours();
                    } else if (obj.toString().indexOf("/") != -1) {
                        return sdf3.parse(obj.toString()).getHours();
                    } else if (obj.toString().indexOf("-") != -1 && obj.toString().length() > 16) {
                        return sdf2.parse(obj.toString()).getHours();
                    } else if (obj.toString().indexOf("-") != -1) {
                        return sdf.parse(obj.toString()).getHours();
                    }
                }
            } catch (Exception e) {
                //解析错误
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 获取季度
     * @param obj
     * @return
     */
    public static String getQuarter(Object obj){
        if(obj != null){
            Integer month = Integer.parseInt(getMonth(obj.toString()));
            if(month < 4){
                return "FIRST_QUARTER";
            }else if(month < 7){
                return "SECOND_QUARTER";
            }else if(month <10){
                return "THIRD_QUARTER";
            }else if(month < 13){
                return "FOURTH_QUARTER";
            }else {
                return null;
            }
        }
        return null;
    }

    /**
     * 计算两个日期的时间差（YYYY-MM-dd）
     * @param date1
     * @param date2
     * @return
     */
    public static Integer dateDifference(String date1,String date2){
//        System.out.println("===="+(date1 +"==="+ date2));
        try {
            if(StringUtils.isBlank(date1) || StringUtils.isBlank(date2)){
                return -1;
            }
            long diff = 0;
            diff = Math.abs(sdf.parse(date1).getTime() - sdf.parse(date2).getTime());
            return  (int) (diff / (24 * 60 * 60 * 1000));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return -1;
    }
}
