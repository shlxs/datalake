package cn.sunrisecolors.datalake.kafka.sync;

import cn.sunrisecolors.datalake.kafka.RecordContext;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author shaohongliang
 * @since 2019/9/5 10:35
 */
@Data
public class SyncRecordContext implements RecordContext {
    private ConsumerRecord<String, String> consumerRecord;

    public SyncRecordContext(ConsumerRecord<String, String> consumerRecord){
        this.consumerRecord = consumerRecord;
    }

    @Override
    public void ack(){
        // do nothing
    }
}
