package cn.sunrisecolors.datalake.kafka.async;

import cn.sunrisecolors.datalake.kafka.RecordContext;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author shaohongliang
 * @since 2019/9/5 10:35
 */
@Data
public class AsyncRecordContext implements RecordContext {
    private ConsumerRecord<String, String> consumerRecord;

    private MarkCompactQueue markCompactQueue;

    private boolean isProcessed;

    public AsyncRecordContext(ConsumerRecord<String, String> consumerRecord, MarkCompactQueue markCompactQueue){
        this.consumerRecord = consumerRecord;
        this.markCompactQueue = markCompactQueue;
    }

    @Override
    public void ack(){
        this.setProcessed(true);
        markCompactQueue.tigger();
    }
}
