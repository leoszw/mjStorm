package com.manji.utils;

public enum PerfixEnum {
    /**
     * 应用前缀 APP:，版本前缀 VERSION:,
     * 年前缀 YEAR:,季度 QUARTER:, 月前缀 MONTH:,周前缀 WEEK:,日前缀 DAY:, 小时 HOUR:,
     * 用户群前缀 USERGROUP:,当前页面 INDEXPAGE:,上级页面 SUPERPAGE:
     * <p>
     * 省份：PROVINCE:，城市：CITY:，用户：USER:
     * <p>
     * 品牌：BRAND:, 设备型号：MODEL:, 联网方式：NETWORKTYPE:, 操作系统：OS:, 分辨率：RESOLUTIONRATIO:
     * <p>
     * 用户标识：DISTINCTID:
     */
    APP("APP:"),
    VERSION("VERSION:"),
    YEAR("YEAR:"),
    QUARTER("QUARTER:"),
    MONTH("MONTH:"),
    WEEK("WEEK:"),
    DAY("DAY:"),
    HOUR("HOUR:"),
    USERGROUP("USERGROUP:"),
    INDEXPAGE("INDEXPAGE:"),
    SUPERPAGE("SUPERPAGE:"),

    PROVINCE("PROVINCE:"),
    CITY("CITY:"),
    USER("USER:"),

    DISTINCTID("DISTINCTID:"),

    BRAND("BRAND:"),
    MODEL("MODEL:"),
    NETWORKTYPE("NETWORKTYPE:"),
    OS("OS:"),
    RESOLUTIONRATIO("RESOLUTIONRATIO:");

    private String code;

    PerfixEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
