package cn.sunrisecolors.datalake.core;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author shaohongliang
 * @since 2019/9/5 11:12
 */
public class Timestamp {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private long timestamp;

    private LocalDateTime localDateTime;

    private String localDateTimeStr;

    public Timestamp(long timestamp) {
        this.timestamp = timestamp;
        this.localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        this.localDateTimeStr = localDateTime.format(DATE_TIME_FORMATTER);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public LocalDateTime getLocalDateTime() {
        return localDateTime;
    }

    public String getLocalDateTimeStr() {
        return localDateTimeStr;
    }

    public String getLocalDateStr(){
        return localDateTimeStr.substring(0, 10);
    }

    public int getYear(){
        return localDateTime.getYear();
    }

    public int getMonth(){
        return localDateTime.getMonthValue();
    }

    public int getDay(){
        return localDateTime.getDayOfMonth();
    }

    public int getHour(){
        return localDateTime.getHour();
    }
}
