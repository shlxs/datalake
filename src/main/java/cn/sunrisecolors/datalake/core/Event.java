package cn.sunrisecolors.datalake.core;

import lombok.Data;

import java.util.Map;

/**
 * @author shaohongliang
 * @since 2019/8/6 13:31
 */
@Data
public class Event {

    // 消息唯一ID，根据Snowflake算法在写入Kafka时生成
    private long uuid;
    // 事件发生的时间戳，精确到毫秒
    private long time;
    // 插入到Kafka的时间戳，精确到毫秒
    private long kafkaInsertTime;
    // 通过time生成

    // 事件名，需是合法的变量名，即不能以数字开头，且只包含：大小写字母、数字、下划线和 $,其中以 $ 开头的表明是系统的保留字段，自定义事件名请不要以 $ 开头
    private String event;
    // 这条数据所属项目名
    private String appId;
    // 这个 Event 的具体属性，其中以$开头的表明是系统的保留字段，它的类型和中文名已经预先定义好了。
    // 自定义属性名需要是合法的变量名，不能以数字开头，且只包含：大小写字母、数字、下划线，自定义属性不能以 $ 开头；
    // 同一个名称的 property，在不同 event 中，必须保持一致的定义和类型；
    // 同一个名称的 property 大小写敏感，如果已经存在小写属性就不可再导入对应大写属性（比如元数据中有 abc 属性名，不能再传 ABC,Abc 等属性名），
    // 否则数据会校验失败不入库。
    private Map<String, Object> properties;
}
