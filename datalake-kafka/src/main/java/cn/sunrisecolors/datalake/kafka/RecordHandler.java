package cn.sunrisecolors.datalake.kafka;

import cn.sunrisecolors.datalake.msgpack.Event;
import cn.sunrisecolors.datalake.msgpack.RecordProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.List;

/**
 * @author shaohongliang
 * @since 2019/8/6 15:14
 */
public class RecordHandler implements Runnable {
    private final RecordContext recordContext;

    private static ObjectMapper mapper = new ObjectMapper();

    public RecordHandler(RecordContext recordContext) {
        this.recordContext = recordContext;
    }

    @Override
    public void run() {
        try {
            ConsumerRecord<String, String> consumerRecord = recordContext.getConsumerRecord();
            Event event = mapper.readValue(consumerRecord.value(), Event.class);
            event.setKafkaInsertTime(consumerRecord.timestamp());
            // 获取事件处理器
            List<RecordProcessor> processorList = RecordProcessor.getProcessorList();

            // 处理Records
            for (RecordProcessor processor : processorList) {
                if(processor.matched(event)){
                    processor.process(event);
                }
            }
            // 标记完成，等待异步提交
            recordContext.ack();
        } catch (IOException e) {
            // error message
        }




    }
}