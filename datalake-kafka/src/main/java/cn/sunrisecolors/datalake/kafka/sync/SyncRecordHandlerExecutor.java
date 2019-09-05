package cn.sunrisecolors.datalake.kafka.sync;

import cn.sunrisecolors.datalake.kafka.RecordContext;
import cn.sunrisecolors.datalake.kafka.RecordHandler;
import cn.sunrisecolors.datalake.kafka.RecordHandlerExecutor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author shaohongliang
 * @since 2019/9/5 10:01
 */
public class SyncRecordHandlerExecutor implements RecordHandlerExecutor {

    @Override
    public void submit(ConsumerRecord<String, String> record) {
        RecordContext context = new SyncRecordContext(record);
        new RecordHandler(context).run();
    }

    @Override
    public void commitOffset(KafkaConsumer<String, String> kafkaConsumer) {
        kafkaConsumer.commitSync();
    }
}
