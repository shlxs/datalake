package cn.sunrisecolors.datalake.time;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * @author shaohongliang
 * @since 2019/9/5 10:53
 */
public class TimeTest {

    @Test
    public void testJsonTime() throws IOException {

        LocalDateTime localDateTime = LocalDateTime.of(2019, 1, 1, 10, 0);
        ObjectMapper mapper = new ObjectMapper();

        String jsonValue = "{\"time\": "+ localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli() + "}";
        TimeObject timeObject = mapper.readValue(jsonValue, TimeObject.class);
        Assert.assertNotEquals(localDateTime, timeObject.getTime());
    }
}
