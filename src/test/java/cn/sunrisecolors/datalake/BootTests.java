package cn.sunrisecolors.datalake;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author :hujiansong
 * @date :2019/8/7 10:02
 * @since :1.8
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class BootTests {

    @Autowired
    KuduClient kuduClient;

    @Test
    public void deleteAllTable() throws KuduException {
        for (String tb : kuduClient.getTablesList().getTablesList()) {
            System.out.println(tb);
            kuduClient.deleteTable(tb);
        }
    }
}
