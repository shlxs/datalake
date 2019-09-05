package cn.sunrisecolors.datalake.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author shaohongliang
 * @since 2019/9/5 9:58
 */
public interface RecordHandlerExecutor {
    void submit(ConsumerRecord<String, String> record);

    void commitOffset(KafkaConsumer<String, String> kafkaConsumer);
}
