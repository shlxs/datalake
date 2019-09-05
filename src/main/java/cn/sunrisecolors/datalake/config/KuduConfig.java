package cn.sunrisecolors.datalake.config;

import org.apache.kudu.client.KuduClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author :hujiansong
 * @date :2019/8/6 15:14
 * @since :1.8
 */
@Configuration
public class KuduConfig {
    @Autowired
    private KuduProperties kuduProperties;

    @Bean
    public KuduClient kuduClient() {
        return new KuduClient.KuduClientBuilder(kuduProperties.getMasterUrl()).build();
    }

}
