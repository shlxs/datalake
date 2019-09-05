package cn.sunrisecolors.datalake.kafka.async;

import cn.sunrisecolors.datalake.kafka.RecordContext;
import cn.sunrisecolors.datalake.kafka.RecordHandler;
import cn.sunrisecolors.datalake.kafka.RecordHandlerExecutor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * maxRunningSize = poolSize 个数 + queue 个数 + 1 (CallerRunsPolicy代表有一个允许在调用线程运行)
 * 由于压缩队列中连续标记完成的数据可以压缩为一条，如 0 0 1 1 1 0 1 --> 0 0 1 0 1，
 *                         假设未完成的有N个（0的个数）,压缩后1的个数最多为1 + N，即1 0 1 0 ...0 1 这种情况,
 *                         此时队列大小最小为2N + 1
 * @author shaohongliang
 * @since 2019/8/8 14:16
 */
public class AsyncRecordHandlerExecutor implements RecordHandlerExecutor {

    private ThreadPoolExecutor executorService;

    private Map<TopicPartition, MarkCompactQueue> partitionStates = new ConcurrentHashMap<>();

    private int compactQueueSize;

    public AsyncRecordHandlerExecutor(int poolSize){
        executorService = new ThreadPoolExecutor(poolSize, poolSize,0L, TimeUnit.MILLISECONDS, new SynchronousQueue(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        int maxRunningSize = poolSize + 1;
        this.compactQueueSize = maxRunningSize * 2 + 1;
    }

    @Override
    public void submit(ConsumerRecord<String, String> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());;
        MarkCompactQueue queue = getQueue(tp);
        AsyncRecordContext context = new AsyncRecordContext(record, queue);

        queue.put(context);
        executorService.submit(new RecordHandler(context));
    }

    private MarkCompactQueue getQueue(TopicPartition tp) {
        MarkCompactQueue queue = partitionStates.get(tp);
        if (queue == null) {
            synchronized (this){
                // double check
                queue = partitionStates.get(tp);
                if(queue == null){
                    queue = new MarkCompactQueue(compactQueueSize);
                    partitionStates.put(tp, queue);
                }
            }
        }
        return queue;
    }

    /**
     * 整个应用共享一个RecordHandlerExecutor实例，需要传入Consumer对象区分不同的消费线程
     * @param kafkaConsumer
     */
    @Override
    public void commitOffset(KafkaConsumer<String, String> kafkaConsumer) {
        Set<TopicPartition> topicPartitions = kafkaConsumer.assignment();

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        List<MarkCompactQueue> allowSlideQueueList = new ArrayList<>();
        for (TopicPartition tp : topicPartitions) {
            MarkCompactQueue queue = partitionStates.get(tp);
            RecordContext context = queue.allowSlideWindowRecord();
            if (context != null) {
                offsets.put(tp, new OffsetAndMetadata(context.getConsumerRecord().offset() + 1));
                allowSlideQueueList.add(queue);
            }
        }
        kafkaConsumer.commitAsync(offsets, null);
        // 提交成功后，滑动窗口滚动
        allowSlideQueueList.forEach(circularQueue -> circularQueue.slideWindow());
    }
}
