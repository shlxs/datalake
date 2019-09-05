package cn.sunrisecolors.datalake.process;

import cn.sunrisecolors.datalake.core.Event;
import cn.sunrisecolors.datalake.core.Timestamp;
import cn.sunrisecolors.datalake.config.ProcessorConfig;
import cn.sunrisecolors.datalake.kafka.RecordProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;

/**
 * 纠正事件时间
 * 判断是否需要纠正，> sys.curtime  => sys.curtime
 * 不在7天内（可配置），=> sys.curtime
 * 将时间转换成日期
 * 加一个date
 *
 * @author shaohongliang
 * @since 2019/8/8 17:13
 */
@Component
@Slf4j
public class TimeProcessor extends RecordProcessor {

    @Autowired
    private ProcessorConfig processorConfig;

    /**
     * 过去几天的时间戳到毫秒
     */
    private int pastDayMills;

    @PostConstruct
    public void initPastDay() {
        pastDayMills = processorConfig.getTime().getTrustPastDay() * 24 * 60 * 60 * 1000;
    }

    @Override
    public void process(Event event) {
        long time = recoverEventTime(event);
        Timestamp eventTime = new Timestamp(time);
        Timestamp kafkaInsertTime = new Timestamp(event.getKafkaInsertTime());

        Map<String, Object> properties = event.getProperties();
        properties.put("date_time", eventTime.getLocalDateTimeStr());
        properties.put("date", eventTime.getLocalDateStr());
        properties.put("hour", eventTime.getHour());
        properties.put("kafka_insert_time", kafkaInsertTime.getLocalDateTimeStr());
    }

    /**
     * 修正SDK上客户端时间
     * @param event
     * @return
     */
    private long recoverEventTime(Event event) {
        long time = event.getTime();
        long curTimeMills = System.currentTimeMillis();
        // 时间比当前时间大  或者 时间小于配置的时间戳
        if (time > curTimeMills || time < curTimeMills - pastDayMills) {
            time = curTimeMills;
            event.setTime(time);
        }
        return time;
    }

}