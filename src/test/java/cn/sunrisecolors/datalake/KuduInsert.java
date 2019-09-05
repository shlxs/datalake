//package cn.sunrisecolors.datalake;
//
//import cn.sunrisecolors.datalake.common.Constants;
//import org.apache.kudu.ColumnSchema;
//import org.apache.kudu.Schema;
//import org.apache.kudu.Type;
//import org.apache.kudu.client.*;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//
///**
// * @author :hujiansong
// * @date :2019/8/15 16:24
// * @since :1.8
// */
//public class KuduInsert {
//
//
//    private int maxNum = 10000;
//    private KuduClient client = new KuduClient.KuduClientBuilder("hadoop-80130").build();
//    private String tableName = "datalake::yk02.device.test";
//    private KuduTable kuduTable;
//
//    private List<String> projectColumns = new ArrayList<>(2);
//
//    {
//        projectColumns.add(Constants.COLUMN_UNIQUE_ID);
//        projectColumns.add(Constants.COLUMN_DEVICE_ID);
//        projectColumns.add(Constants.COLUMN_APP_ID);
//        projectColumns.add("test");
//
//    }
//
//    private List<ColumnSchema> defaultColumns = new ArrayList<>();
//
//    {
//        defaultColumns.add(new ColumnSchema.ColumnSchemaBuilder(Constants.COLUMN_UNIQUE_ID, Type.STRING).key(true).build());
//        defaultColumns.add(new ColumnSchema.ColumnSchemaBuilder(Constants.COLUMN_DEVICE_ID, Type.STRING).key(true).build());
//        defaultColumns.add(new ColumnSchema.ColumnSchemaBuilder(Constants.COLUMN_APP_ID, Type.STRING).key(true).build());
//        defaultColumns.add(new ColumnSchema.ColumnSchemaBuilder("test", Type.STRING).nullable(true).build());
//
//    }
//
//    private Schema schema = new Schema(defaultColumns);
//
//    public KuduInsert() throws KuduException {
//    }
//
//    @Before
//    public void createTestTable() throws KuduException {
//        CreateTableOptions options = new CreateTableOptions().setNumReplicas(1)
//                .addHashPartitions(Collections.singletonList(Constants.COLUMN_UNIQUE_ID), 16);
//
//        try {
//            client.createTable(tableName, schema, options);
//            kuduTable = client.openTable(tableName);
//        } catch (Exception e) {
//            kuduTable = client.openTable(tableName);
//        }
//    }
//
//    @Test
//    public void directInsert() throws KuduException {
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < maxNum; i++) {
//            doInsert();
//        }
//        long end = System.currentTimeMillis();
//        System.out.println("directInsert cost " + (end - start));
//    }
//
//    @Test
//    public void existInsert() throws KuduException {
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < maxNum; i++) {
//            if (!exist()) {
//                doInsert();
//            }
//        }
//        long end = System.currentTimeMillis();
//        System.out.println("existInsert cost " + (end - start));
//    }
//
//    private boolean exist() throws KuduException {
//        KuduPredicate kuduPredicate = KuduPredicate.newComparisonPredicate(schema.getColumn(Constants.COLUMN_UNIQUE_ID),
//                KuduPredicate.ComparisonOp.EQUAL, "device-2yk02");
//        KuduScanner scanner = client.newScannerBuilder(kuduTable)
//                .setProjectedColumnNames(projectColumns)
//                .addPredicate(kuduPredicate)
//                .build();
//        return scanner.hasMoreRows();
//    }
//
//
//    @After
//    public void deleteTable() throws KuduException {
//        client.deleteTable(tableName);
//    }
//
//    private void doInsert() throws KuduException {
//        KuduSession kuduSession = client.newSession();
//        Insert insert = kuduTable.newInsert();
//        PartialRow row = insert.getRow();
//        row.addString(Constants.COLUMN_UNIQUE_ID, "device-2yk02");
//        row.addString(Constants.COLUMN_DEVICE_ID, "device-2");
//        row.addString(Constants.COLUMN_APP_ID, "yk03");
//        row.addString("test", "yk03-test");
//        kuduSession.apply(insert);
//        kuduSession.close();
//    }
//
//}
