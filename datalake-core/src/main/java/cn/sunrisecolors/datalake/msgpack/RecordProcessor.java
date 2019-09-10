package cn.sunrisecolors.datalake.msgpack;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author shaohongliang
 * @since 2019/8/8 16:26
 */
public abstract class RecordProcessor implements Comparable<RecordProcessor>{

    private static final List<RecordProcessor> processorList = new ArrayList<>();

    private static final int DEFAULT_ORDER = 100;

    public RecordProcessor() {
        // 每个Processor初始化时注册自己
        processorList.add(this);
        Collections.sort(processorList);
    }

    public static List<RecordProcessor> getProcessorList() {
        return processorList;
    }

    public abstract void process(Event event);

    /**
     * 定义Processor优先级，值越大优先级越高
     * 默认为Integer最大值一半
     * @return
     */
    public int getOrder(){
        return DEFAULT_ORDER;
    }

    @Override
    public int compareTo(RecordProcessor other) {
        return this.getOrder() - other.getOrder();
    }

    public boolean matched(Event event){
        return true;
    };

}
