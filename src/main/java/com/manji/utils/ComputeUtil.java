package com.manji.utils;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: szw
 * Date: 2019-09-27
 * Time: 18:43
 */
public class ComputeUtil {
    /**
     * 计算比例
     *
     * @param fz
     * @param fm
     * @return
     */
    public static Double device(Object fz, Object fm) {
        Double db = new Double(0);
        try {
            if (fz != null && fm != null && Double.parseDouble(fm + "") != new Double(0)) {
                db = Double.parseDouble(fz.toString()) / Double.parseDouble(fm.toString());
            }
        } catch (Exception e) {

        }
        return db;
    }
}
