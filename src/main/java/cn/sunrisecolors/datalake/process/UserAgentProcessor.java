package cn.sunrisecolors.datalake.process;

import cn.sunrisecolors.datalake.core.Event;
import cn.sunrisecolors.datalake.kafka.RecordProcessor;
import cn.sunrisecolors.datalake.process.ua.Client;
import cn.sunrisecolors.datalake.process.ua.Parser;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.Map;

/**
 * UA
 * 提取终端类型
 *
 * @author shaohongliang
 * @since 2019/8/8 16:36
 */
@Component
public class UserAgentProcessor extends RecordProcessor {
    private Parser parser;

    @PostConstruct
    public void init() {
        try {
            parser = new Parser();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(Event event) {
        Map<String, Object> prop = event.getProperties();
        String ua = (String) prop.get("user-agent");
        if (!StringUtils.isEmpty(ua)) {
            final Client uaInfo = parser.parse(ua);

            // browser
            if (!StringUtils.isEmpty(uaInfo.userAgent.family)) {
                prop.put("browser", uaInfo.userAgent.family);
            }

            // browser_version
            if (!StringUtils.isEmpty(uaInfo.userAgent.major)) {
                StringBuilder bv = new StringBuilder();
                bv.append(uaInfo.userAgent.major);
                if (!StringUtils.isEmpty(uaInfo.userAgent.major)) {
                    bv.append(".").append(uaInfo.userAgent.major);
                    if (!StringUtils.isEmpty(uaInfo.userAgent.patch)) {
                        bv.append(".").append(uaInfo.userAgent.patch);
                    }
                }
                prop.put("browser_version", bv.toString());
            }

            // system
            if (!StringUtils.isEmpty(uaInfo.os.family)) {
                prop.put("system", uaInfo.os.family);
            }

            // system_version
            if (!StringUtils.isEmpty(uaInfo.os.major)) {
                StringBuilder sv = new StringBuilder();
                sv.append(uaInfo.os.major);
                if (!StringUtils.isEmpty(uaInfo.os.major)) {
                    sv.append(".").append(uaInfo.os.major);
                    if (!StringUtils.isEmpty(uaInfo.os.patch)) {
                        sv.append(".").append(uaInfo.os.patch);
                    }
                }
                prop.put("system_version", sv.toString());
            }

            // device
            if (!StringUtils.isEmpty(uaInfo.device.family)) {
                prop.put("device", uaInfo.device.family);
            }
        }

    }


}