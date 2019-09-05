package cn.sunrisecolors.datalake.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author :hujiansong
 * @date :2019/6/19 17:44
 * @since :1.8
 */
@Configuration
@ConfigurationProperties(prefix = "kafka")
@Data
public class KafkaProperties {

    /**
     * kafka brokerlist
     */
    private String brokerList;

    /**
     * kafka consumer groupId
     */
    private String groupId;

    /**
     * Kafka topic
     */
    private String topic;


    /**
     * consumer fetcher thread nums
     */
    private int fetcherThreadNum = Runtime.getRuntime().availableProcessors();


    /**
     * pool timeout milliseconds, also is offset commit duration
     */
    private long pollTimeoutMs = 30000;


    /**
     * use async thread pool process kafka record
     */
    private boolean asyncProcess = true;


    /**
     * record handler thread nums
     */
    private int asyncProcessThreadNum;

}
