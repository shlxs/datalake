package cn.sunrisecolors.datalake.process;

import cn.sunrisecolors.datalake.core.Constants;
import cn.sunrisecolors.datalake.core.Event;
import cn.sunrisecolors.datalake.kafka.RecordProcessor;
import cn.sunrisecolors.datalake.kudu.KuduDocumentService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

/**
 * @author shaohongliang
 * @since 2019/8/28 11:53
 */
public abstract class AbstractKuduProcessor extends RecordProcessor {
    @Autowired
    private KuduDocumentService kuduService;

    @Override
    public int getOrder() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void process(Event event) {
        KTable kuduTable = getTable(event);
        String id = getId(event);

        Map<String, Object> source = event.getProperties();
        source.put(Constants.Column.APP_ID, event.getAppId());
        source.put(Constants.Column.EVENT, event.getEvent());

        boolean loop = true;
        while (loop) {
            try {
                if (kuduTable.isOverwrite()) {
                    kuduService.upsert(kuduTable.getName(), id, source);
                } else if (kuduTable.checkExist()) {
                    if (!kuduService.exist(kuduTable.getName(), id)) {
                        kuduService.insert(kuduTable.getName(), id, source);
                    }
                } else {
                    kuduService.insert(kuduTable.getName(), id, source);
                }
                loop = false;
            } catch (Exception e) {

            }
        }
    }

    protected abstract KTable getTable(Event event);

    protected abstract String getId(Event event);

    enum KTable {
        EVENTS("events"),
        ITEMS("items", true, false),
        LAUNCH("launch"),
        LOGIN("login"),
        REGISTER("register"),
        RECHARGE("recharge"),
        ACTIVATE("activate", false, true);

        private String name;
        private boolean isOverwrite;
        private boolean checkExist;

        KTable(String name) {
            this(name, false, false);
        }

        KTable(String name, boolean isOverwrite, boolean checkExist) {
            this.name = name;
            this.isOverwrite = isOverwrite;
            this.checkExist = checkExist;
        }

        public String getName() {
            return name;
        }

        public boolean isOverwrite() {
            return isOverwrite;
        }

        public boolean checkExist() {
            return checkExist;
        }

        public static KTable get(String name) {
            KTable kuduTable;
            try {
                kuduTable = KTable.valueOf(name);
            } catch (IllegalArgumentException e) {
                // TODO:: 没有找到会发生IllegalArgumentException
                kuduTable = KTable.EVENTS;
            }
            return kuduTable;
        }

    }
}
