package cn.sunrisecolors.datalake.process;

import cn.sunrisecolors.datalake.core.Event;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author :hujiansong
 * @date :2019/8/6 16:22
 * @since :1.8
 */
@Component
@Slf4j
public class KuduEventProcessor extends AbstractKuduProcessor {


    @Override
    public KTable getTable(Event event) {
        String eventName = event.getEvent().toLowerCase();
        KTable kuduTable = KTable.get(eventName);
        return kuduTable;
    }

    @Override
    protected String getId(Event event) {
        return String.valueOf(event.getUuid());
    }


}
