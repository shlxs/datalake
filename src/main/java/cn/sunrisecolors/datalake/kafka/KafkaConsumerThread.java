package cn.sunrisecolors.datalake.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;

/**
 * 消费者线程，根据KafkaProperties.getFetcherThreadNum()启动多个实例
 * @author shaohongliang
 * @since 2019/8/8 14:01
 */
@Slf4j
public class KafkaConsumerThread extends Thread {
    private final KafkaConsumer<String, String> kafkaConsumer;

    private final RecordHandlerExecutor executor;

    private long pollTimeoutMs;

    public KafkaConsumerThread(RecordHandlerExecutor executor, KafkaConsumer<String, String> kafkaConsumer, long pollTimeoutMs) {
        this.pollTimeoutMs = pollTimeoutMs;
        this.kafkaConsumer = kafkaConsumer;
        this.executor = executor;
    }

    @Override
    public void run() {
        try {
            while (true) {
                // 超过pollTimeoutMs后poll方法会返回
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(pollTimeoutMs));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        // 将每条消息记录分发到线程池执行
                        executor.submit(record);
                    }
                }
                // 检查并提交偏移量
                executor.commitOffset(kafkaConsumer);
            }
        } catch (Exception e) {
            log.error("kafka take failed", e);
        } finally {
            kafkaConsumer.close();
        }
    }
}
