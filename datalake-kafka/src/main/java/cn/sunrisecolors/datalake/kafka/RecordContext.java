package cn.sunrisecolors.datalake.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;

/*
 *包装类, 封装了Consumer对象供提交Offset时使用
 * @author shaohongliang
 * @since 2019/8/8 15:42
 */
public interface RecordContext{
    ConsumerRecord<String, String> getConsumerRecord();

    void ack();
}
