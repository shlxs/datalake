package cn.sunrisecolors.datalake.process;

import cn.sunrisecolors.datalake.core.Constants;
import cn.sunrisecolors.datalake.core.Event;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 提取用户新增设备
 *
 * @author shaohongliang
 * @since 2019/8/8 16:31
 */
@Component
@Slf4j
public class KuduDeviceProcessor extends AbstractKuduProcessor {

    @Override
    public boolean matched(Event event) {
        String eventName = event.getEvent();
        return Constants.Event.LAUNCH.equals(eventName);
    }

    @Override
    public KTable getTable(Event event) {
        return KTable.ACTIVATE;
    }

    @Override
    protected String getId(Event event) {
        String deviceId = (String) event.getProperties().get(Constants.Column.DEVICE_ID);
        return deviceId;
    }

}
