package cn.sunrisecolors.datalake.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author :hujiansong
 * @date :2019/8/15 15:59
 * @since :1.8
 */
@Configuration
@ConfigurationProperties(prefix = "processor")
@Data
public class ProcessorConfig {

    private IP ip = new IP();

    private Time time = new Time();

    @Data
    public static class IP {
        private String fileUrl;
        private Integer intervalSecond;
    }

    @Data
    public static class Time {
        private Integer trustPastDay;
    }
}
