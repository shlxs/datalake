package cn.sunrisecolors.datalake.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author :hujiansong
 * @date :2019/8/15 15:58
 * @since :1.8
 */
@Configuration
@ConfigurationProperties(prefix = "kudu")
@Data
public class KuduProperties {
    /**
     * kudu master url
     */
    private String masterUrl;
}
