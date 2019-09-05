package cn.sunrisecolors.datalake.kudeException;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author :hujiansong
 * @date :2019/9/5 15:50
 * @since :1.8
 */
public class KuduInsertUnitTest {

    KuduClient client;

    @Before
    public void buildClient() {
        client = new KuduClient.KuduClientBuilder("hadoop-80130").build();

        ColumnSchema id = new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).key(true).nullable(false).build();
        ColumnSchema val1 = new ColumnSchema.ColumnSchemaBuilder("val1", Type.STRING).key(false).nullable(true).build();

        Schema schema = new Schema(Arrays.asList(id, val1));

        CreateTableOptions cto = new CreateTableOptions();
        cto.addHashPartitions(Arrays.asList("id"), 16);
        try {
            client.createTable("insertTableTest", schema, cto);
        } catch (KuduException e) {
            // noop
        }
    }

    /**
     * throws NonRecoverableException: Primary key column id is not set
     */
    @Test
    public void insertIdNull() {
        try {
            KuduTable kuduTable = client.openTable("insertTableTest");
            KuduSession kuduSession = client.newSession();
            Insert insert = kuduTable.newInsert();
            PartialRow row = insert.getRow();
            row.addString("val1", "123");
//            row.addString("id", null);
            OperationResponse apply = kuduSession.apply(insert);

        } catch (KuduException e) {
            e.printStackTrace();
        }


    }


    /**
     * 错误在OperationResponse中, Row error for primary key="1"
     */
    @Test
    public void dataExist() {
        try {
            KuduTable kuduTable = client.openTable("insertTableTest");
            KuduSession kuduSession = client.newSession();
            Insert insert = kuduTable.newInsert();
            PartialRow row = insert.getRow();
            row.addString("id", "1");
            row.addString("val2", "123");
//            row.addString("id", null);

            OperationResponse apply = kuduSession.apply(insert);
            if (apply.hasRowError()) {
                throw new RuntimeException(apply.getRowError().toString());
            }
            kuduSession.flush();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }


    /**
     * IllegalArgumentException: Unknown column: val2
     */
    @Test
    public void columnExist() {
        try {
            KuduTable kuduTable = client.openTable("insertTableTest");
            KuduSession kuduSession = client.newSession();
            Insert insert = kuduTable.newInsert();
            PartialRow row = insert.getRow();
            row.addString("id", "2");
            row.addString("val2", "123");
//            row.addString("id", null);
            OperationResponse apply = kuduSession.apply(insert);
            if (apply.hasRowError()) {
                throw new RuntimeException(apply.getRowError().toString());
            }
            kuduSession.flush();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}
