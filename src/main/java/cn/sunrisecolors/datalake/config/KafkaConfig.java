package cn.sunrisecolors.datalake.config;

import cn.sunrisecolors.datalake.kafka.KafkaConsumerThread;
import cn.sunrisecolors.datalake.kafka.RecordHandlerExecutor;
import cn.sunrisecolors.datalake.kafka.async.AsyncRecordHandlerExecutor;
import cn.sunrisecolors.datalake.kafka.sync.SyncRecordHandlerExecutor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author :hujiansong
 * @date :2019/8/6 15:14
 * @since :1.8
 */
@Configuration
public class KafkaConfig {
    @Autowired
    private KafkaProperties kafkaProperties;

    /**
     * 消费者配置
     * @return
     */
    @Bean
    public Properties consumerProps() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBrokerList());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getGroupId());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(false));
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return props;
    }

    @Bean
    public RecordHandlerExecutor recordHandlerExecutor() {
        if(kafkaProperties.isAsyncProcess()){
            return new AsyncRecordHandlerExecutor(kafkaProperties.getAsyncProcessThreadNum());
        }else{
            return new SyncRecordHandlerExecutor();
        }
    }

    @Bean
    public List<KafkaConsumerThread> kafkaConsumerThreads(Properties consumerProps, RecordHandlerExecutor executor) {
        List<KafkaConsumerThread> threadList = new ArrayList<>();
        for (int i = 0; i < kafkaProperties.getFetcherThreadNum(); i++) {
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
            kafkaConsumer.subscribe(Collections.singletonList(kafkaProperties.getTopic()));
            threadList.add(new KafkaConsumerThread(executor, kafkaConsumer, kafkaProperties.getPollTimeoutMs()));
        }
        return threadList;
    }

}
