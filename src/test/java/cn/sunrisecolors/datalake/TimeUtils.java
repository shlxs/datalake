package cn.sunrisecolors.datalake;

import org.junit.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;

/**
 * @author :hujiansong
 * @date :2019/8/28 16:57
 * @since :1.8
 */
public class TimeUtils {


    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");


    @Test
    public void test() {
        char minByte = '\u0000';
        System.out.println("==" + minByte + "==");
    }


    @Test
    public void nextMonthDay() {
        LocalDate now = LocalDate.now().withMonth(1).plusMonths(1).withDayOfMonth(1);
        System.out.println(now.format(DATE_FMT));
    }

    @Test
    public void times() {
        LocalDate now = LocalDate.now().withDayOfMonth(31);
        LocalDate lastDay = now.with(TemporalAdjusters.lastDayOfMonth());

        long days = now.until(lastDay, ChronoUnit.DAYS) + 1;
        System.out.println(days);

        for (int i = 0; i < days; i++) {
            System.out.println(now.format(DATE_FMT));

            now = now.plusDays(1);

            System.out.println(now.format(DATE_FMT));
        }

    }
}
