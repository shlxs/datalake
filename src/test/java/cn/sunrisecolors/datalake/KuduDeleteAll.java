//package com.dobest.datalake;
//
//import com.dobest.datalake.common.Constants;
//import org.apache.kudu.ColumnSchema;
//import org.apache.kudu.Schema;
//import org.apache.kudu.Type;
//import org.apache.kudu.client.*;
//import org.apache.kudu.shaded.com.google.common.collect.Sets;
//import org.junit.Test;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.Set;
//import java.util.concurrent.atomic.AtomicInteger;
//
///**
// * @author :hujiansong
// * @date :2019/8/15 16:24
// * @since :1.8
// */
//public class KuduDeleteAll {
//
//    @Test
//    public void delete() throws KuduException {
//        KuduClient client = new KuduClient.KuduClientBuilder("hadoop-80130").build();
//
//        client.deleteTable("p-1");
//    }
//
//    @Test
//    public void getPartition() throws Exception {
//        KuduClient client = new KuduClient.KuduClientBuilder("hadoop-80130").build();
//
//        KuduTable kuduTable = client.openTable("p-1");
//        List<String> ps = kuduTable.getFormattedRangePartitions(3000);
//        for (String e : ps) {
//            System.out.println(e);
//        }
//
//    }
//
//    @Test
//    public void alterPartition() {
//        Set<String> singleTableEvents = Sets.newHashSet(
//                Constants.EVENT_LAUNCH, Constants.EVENT_LOGIN,
//                Constants.EVENT_REGISTER, Constants.EVENT_RECHARGE, "events", "item", "active");
//        AlterTableOptions ato = new AlterTableOptions();
//
//        KuduClient client = new KuduClient.KuduClientBuilder("es").build();
//
//        for (String e : singleTableEvents) {
//            try {
//                client.alterTable(e, ato);
//                System.out.println(e + " alter success");
//            } catch (KuduException e1) {
//                e1.printStackTrace();
//                System.out.println(e + " not alter success");
//            }
//        }
//    }
//
//    @Test
//    public void delete111() throws Exception {
//        KuduClient client = new KuduClient.KuduClientBuilder("es").build();
//        client.deleteTable("events");
//        client.deleteTable("active");
//    }
//
//    @Test
//    public void createTable1() throws Exception {
//        KuduClient client = new KuduClient.KuduClientBuilder("es").build();
////        KuduClient client = new KuduClient.KuduClientBuilder("hadoop-80130").build();
//        ColumnSchema cs = new ColumnSchema.ColumnSchemaBuilder(Constants.COLUMN_DATE, Type.STRING).key(true).build();
//        ColumnSchema cs1 = new ColumnSchema.ColumnSchemaBuilder(Constants.COLUMN_ID, Type.STRING).key(true).build();
//        ColumnSchema cs2 = new ColumnSchema.ColumnSchemaBuilder("val", Type.STRING).key(false).build();
//        Schema schema = new Schema(Arrays.asList(cs, cs1, cs2));
//
//        CreateTableOptions cto = new CreateTableOptions().setNumReplicas(1);
//        cto.addHashPartitions(Arrays.asList(Constants.COLUMN_ID), 16);
//        cto.addRangePartition(startKey(schema), endKey(schema));
//        cto.setRangePartitionColumns(Arrays.asList(Constants.COLUMN_DATE));
//
//        KuduTable table = client.createTable("p-1", schema, cto);
//    }
//
//    public static void main(String[] args) throws KuduException {
//        KuduClient client = new KuduClient.KuduClientBuilder("es").build();
////        KuduClient client = new KuduClient.KuduClientBuilder("hadoop-80130").build();
//        ColumnSchema cs = new ColumnSchema.ColumnSchemaBuilder(Constants.COLUMN_DATE, Type.STRING).key(true).build();
//        ColumnSchema cs1 = new ColumnSchema.ColumnSchemaBuilder(Constants.COLUMN_ID, Type.STRING).key(true).build();
//        ColumnSchema cs2 = new ColumnSchema.ColumnSchemaBuilder("val", Type.STRING).key(false).build();
//        Schema schema = new Schema(Arrays.asList(cs, cs1, cs2));
//
//        CreateTableOptions cto = new CreateTableOptions().setNumReplicas(1);
//        cto.addHashPartitions(Arrays.asList(Constants.COLUMN_ID), 16);
//        cto.addRangePartition(startKey(schema), endKey(schema));
//        cto.setRangePartitionColumns(Arrays.asList(Constants.COLUMN_DATE));
//
//        KuduTable table = client.createTable("p-1", schema, cto);
//        System.out.println("create table p-1 success");
//
//        KuduSession kuduSession = client.newSession();
//        Insert insert = table.newInsert();
//        PartialRow row = insert.getRow();
//        row.addString(Constants.COLUMN_ID, "1");
//        row.addString(Constants.COLUMN_DATE, "2019-08-28");
//        row.addString("val", "2019-08-28");
//        kuduSession.apply(insert);
//        kuduSession.close();
//
//        System.out.println("insert 2019-08-28");
//
//        insertNext(client, table);
//        System.out.println("insert 2019-08-29");
//
//
//        System.out.println("=========== 此时表中  28  29");
//
//
//        System.out.println("alter table p-1 add range partition");
//
////        AlterTableOptions ato = new AlterTableOptions();
////        ato.addRangePartition(startKey(schema), endKey(schema));
////        client.alterTable("p-1", ato);
//
//        System.out.println("alter table success");
//
//        insertNext(client, table);
//
//        System.out.println("=========== 此时表中  28  29");
//
////        AlterTableOptions ato1 = new AlterTableOptions();
////        ato1.addRangePartition(endKey(schema), endKey2(schema));
////        client.alterTable("p-1", ato1);
//
//        insertNext(client, table);
//        System.out.println("=========== 此时表中  28  29 29");
//
//
////        KuduClient client = new KuduClient.KuduClientBuilder("es").build();
////        client.deleteTable("datalake::yk02.device");
////        client.deleteTable("datalake::yk01.device");
////        client.deleteTable("datalake::yk03.device");
////        client.deleteTable("datalake::yk01.events");
////        client.deleteTable("datalake::yk02.events");
////        client.deleteTable("datalake::yk02.events");
////        client.deleteTable("datalake::yk03.events");
////        client.deleteTable("datalake::yk02.item");
////        try {
////            client.deleteTable("datalake::yk01.events");
////        } catch (Exception e) {
////
////        }
////        try {
////            client.deleteTable("datalake::yk02.events");
////        } catch (Exception e) {
////
////        }
////
////        try {
////            client.deleteTable("datalake::yk03.events");
////        } catch (Exception e) {
////
////        }
//    }
//
//
//    private static PartialRow endKey2(Schema schema) {
//        PartialRow row = new PartialRow(schema);
//        row.addString(Constants.COLUMN_DATE, "2019-08-30");
//        return row;
//    }
//
//    private static AtomicInteger i = new AtomicInteger(1);
//
//    private static void insertNext(KuduClient client, KuduTable table) throws KuduException {
//        KuduSession kuduSession1 = client.newSession();
//        Insert insert1 = table.newInsert();
//        PartialRow row1 = insert1.getRow();
//        row1.addString(Constants.COLUMN_ID, "22" + i.getAndIncrement());
//        row1.addString(Constants.COLUMN_DATE, "2019-08-29");
//        row1.addString("val", "2019-08-29" + i.getAndIncrement());
//        kuduSession1.apply(insert1);
//        kuduSession1.close();
//    }
//
//    private static PartialRow endKey(Schema schema) {
//        PartialRow row = new PartialRow(schema);
//        row.addString(Constants.COLUMN_DATE, "2019-08-29");
//        return row;
//    }
//
//    private static PartialRow startKey(Schema schema) {
//        PartialRow row = new PartialRow(schema);
//        row.addString(Constants.COLUMN_DATE, "2019-08-28");
//        return row;
//    }
//}
