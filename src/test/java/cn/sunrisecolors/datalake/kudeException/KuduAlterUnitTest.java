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
 * @date :2019/9/5 9:51
 * @since :1.8
 */
public class KuduAlterUnitTest {

    KuduClient client;

    @Before
    public void buildClient() {
        client = new KuduClient.KuduClientBuilder("hadoop-80130").build();

    }


    /**
     * throws NonRecoverableException: The table does not exist: table_name: "test-not-exist"
     */
    @Test
    public void tableNotExist() {
        try {
            KuduTable kuduTable = client.openTable("test-not-exist");
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }


    /**
     * throws NonRecoverableException: New range partition conflicts with existing range partition:
     */
    @Test
    public void partitionExist() {
        try {
            ColumnSchema date = new ColumnSchema.ColumnSchemaBuilder("item_date", Type.STRING).key(true).nullable(false).build();
            ColumnSchema id = new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).key(true).nullable(false).build();

            Schema schema = new Schema(Arrays.asList(date, id));
            AlterTableOptions ato = new AlterTableOptions();
            PartialRow start = new PartialRow(schema);
            start.addString("item_date", "2019-09-05");

            PartialRow end = new PartialRow(schema);
            end.addString("item_date", "2019-09-05");
            ato.addRangePartition(start, end, RangePartitionBound.INCLUSIVE_BOUND, RangePartitionBound.INCLUSIVE_BOUND);
            client.alterTable("rangePartitionTest1", ato);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }


}
