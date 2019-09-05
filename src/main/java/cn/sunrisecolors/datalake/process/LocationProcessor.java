package cn.sunrisecolors.datalake.process;

import cn.sunrisecolors.datalake.core.Event;
import cn.sunrisecolors.datalake.config.ProcessorConfig;
import cn.sunrisecolors.datalake.kafka.RecordProcessor;
import cn.sunrisecolors.datalake.process.ip.AutoReloadNetLocator;
import cn.sunrisecolors.datalake.process.ip.LocationInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;


/**
 * ip转省市
 *
 * @author shaohongliang
 * @since 2019/8/8 16:34
 */
@Component
public class LocationProcessor extends RecordProcessor {

    private AutoReloadNetLocator ipLoader;

    @Autowired
    private ProcessorConfig processorConfig;

    @PostConstruct
    public void init() {
        try {
            ipLoader = new AutoReloadNetLocator(processorConfig.getIp().getFileUrl(), processorConfig.getIp().getIntervalSecond());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(Event event) {
        String ip = (String) event.getProperties().get("$ip");
        LocationInfo locationInfo = ipLoader.find(ip);
        if (locationInfo != null) {
            Map<String, Object> prop = event.getProperties();
            prop.put("country", locationInfo.getCountry());
            prop.put("province", locationInfo.getProvince());
            prop.put("city", locationInfo.getCity());
        }
    }

}