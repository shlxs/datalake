package cn.sunrisecolors.datalake;

import cn.sunrisecolors.datalake.kafka.KafkaConsumerThread;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.List;

/**
 * @author hujiansong
 */
@SpringBootApplication
public class BootApplication {

    public static void main(String[] args) {
        SpringApplication.run(BootApplication.class, args);
    }


    /**
     * 启动消费者线程
     * @param kafkaConsumerThreads
     * @return
     */
    @Bean
    CommandLineRunner init(List<KafkaConsumerThread> kafkaConsumerThreads) {
        return (args) -> {
            for (KafkaConsumerThread consumerThread : kafkaConsumerThreads) {
                consumerThread.start();
            }
        };
    }
}
