package cn.sunrisecolors.datalake.core;

/**
 * @author :hujiansong
 * @date :2019/8/19 9:54
 * @since :1.8
 */
public final class Constants {
    /**
     * 内置事件名称
     */
    public interface Event{
        public static final String LAUNCH = "launch";             // 启动
        public static final String REGISTER = "register";         // 注册
        public static final String EVENT_LOGIN = "login";               // 登陆
        public static final String EVENT_LOGIN_RECHARGE = "recharge";         // 付费
    }

    /**
     * 系统内置列
     */
    public interface Column{
        public static final String DEVICE_ID = "device_id";             // 启动
        public static final String APP_ID = "app_id";;         // 注册
        public static final String EVENT = "event";               // 登陆
        public static final String EVENT_LOGIN_RECHARGE = "recharge";         // 付费
    }


//    /**
//     * event default key
//     */
//    public static final String COLUMN_UUID = "uuid";
//    public static final String COLUMN_EVENT = "event";
//    public static final String COLUMN_DATE = "date";
//    public static final String COLUMN_TIME = "time";
//
//
//    /**
//     * device  column key
//     */
//    public static final String COLUMN_ID = "id";
//    public static final String COLUMN_UNIQUE_ID = "unique_id";
    public static final String COLUMN_DEVICE_ID = "device_id";
    public static final String COLUMN_APP_ID = "app_id";
//    public static final String DEVICE_KEY = "$device";
//
//
//    /**
//     * user-agent constant
//     */
//    public static final String UA_KEY = "user-agent";
//    public static final String UA_BROWSER = "browser";
//    public static final String UA_BROWSER_VERSION = "browser_version";
//    public static final String UA_SYSTEM = "system";
//    public static final String UA_SYSTEM_VERSION = "system_version";
//    public static final String UA_DEVICE = "device";
//
//
//    /**
//     * location constant
//     */
//    public static final String LOCATION_KEY = "$ip";
//    public static final String LOCATION_COUNTRY = "country";
//    public static final String LOCATION_PROVINCE = "province";
//    public static final String LOCATION_CITY = "city";


}
