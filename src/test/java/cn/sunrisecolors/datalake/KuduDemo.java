package cn.sunrisecolors.datalake;

import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.kudu.shaded.com.google.common.cache.CacheBuilder;
import org.apache.kudu.shaded.com.google.common.cache.CacheLoader;
import org.apache.kudu.shaded.com.google.common.cache.LoadingCache;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author shaohongliang
 * @since 2019/8/6 16:53
 */
public class KuduDemo {
    public static void main(String[] args) throws Exception {
        // open client
        KuduClient client = new KuduClient.KuduClientBuilder("hadoop-80128:7051,hadoop-80129:7051,hadoop-80130:7051").build();
        LoadingCache<String, Optional<KuduTable>> kuduTableCache = CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build(
                new CacheLoader<String, Optional<KuduTable>>() {
                    @Override
                    public Optional<KuduTable> load(String tableName) throws Exception {
                        if(client.tableExists(tableName)){
                            return Optional.of(client.openTable(tableName));
                        }else{
                            return Optional.empty();
                        }
                    }
                }
        );
        Optional<KuduTable> table = kuduTableCache.get("impala::advertise.test_partition1");
        ;
        System.out.println(table.isPresent());

        kuduTableCache.refresh("impala::advertise.test_partition1");

        KuduTable kuduTable = table.get();

        AlterTableOptions ato = new AlterTableOptions();
        Schema schema = new Schema(kuduTable.getSchema().getPrimaryKeyColumns());
        PartialRow startKey = new PartialRow(schema);
        startKey.addString("app_id", "ac");

        PartialRow endKey = schema.newPartialRow();
        endKey.addString("app_id", "ac");
        //"ac\000" <= VALUES < "\000\000"
        //"ac" <= VALUES < "\000\000\000"
        ato.addRangePartition(startKey, endKey, RangePartitionBound.INCLUSIVE_BOUND, RangePartitionBound.INCLUSIVE_BOUND);

        client.alterTable(kuduTable.getName(), ato);
//
//        Insert insert = kuduTable.newInsert();
//        PartialRow row = insert.getRow();
//
//        kuduTable.getSchema().getColumn("id").getType();
//        row.addString("id", "1");
//        row.addString("app_id", "1");
//        row.addString("day", "1");
//
//        KuduSession session = client.newSession();
//        OperationResponse response = session.apply(insert);
//        if(response.hasRowError()){
//            RowError rowError = response.getRowError();
//            rowError.getErrorStatus().isNotFound();
//        }
//        System.out.println(response);

//
//        ThreadPoolExecutor executorService = new ThreadPoolExecutor(1, 1,
//                0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1),
//                new ThreadPoolExecutor.CallerRunsPolicy());
//        executorService.submit(new Runnable() {
//            @Override
//            public void run() {
//                System.out.println(Thread.currentThread().getName() + ": thread1" );
//                try {
//                    Thread.sleep(10000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        executorService.submit(new Runnable() {
//            @Override
//            public void run() {
//                System.out.println(Thread.currentThread().getName() + ": thread2" );
//                try {
//                    Thread.sleep(10000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        executorService.submit(new Runnable() {
//            @Override
//            public void run() {
//                System.out.println(Thread.currentThread().getName() + ": thread3" );
//                try {
//                    Thread.sleep(10000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        System.out.println("ff");
//        List<String> rangePartitions = new ArrayList<>();
//        for (LocatedTablet tablet : kuduTable.getTabletsLocations(0)) {
//            Partition partition = tablet.getPartition();
//            // Filter duplicate range partitions by taking only the tablets whose hash
//            // partitions are all 0s.
//            if (!Iterators.all(partition.getHashBuckets().iterator(), Predicates.equalTo(0))) {
//                continue;
//            }
//
//            String start = new String(partition.getRangeKeyStart());
//            String end = new String(partition.getRangeKeyEnd());
//            System.out.println(start + "," + end);
//        }


//        String tableName = "test";
//        // create table
//        List<ColumnSchema> columns = new ArrayList<>();
//        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).key(true).build());
//        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build());
//        Schema schema = new Schema(columns);
//
//        List<String> rangeKeys = new ArrayList<>();
//        rangeKeys.add("key");
//
//        try {
//            client.createTable(tableName, schema, new CreateTableOptions().setRangePartitionColumns(rangeKeys));
//        }catch (KuduException e){
//            e.getStatus().isAlreadyPresent();
//        }
//
//        // open table
//        KuduTable table = client.openTable(tableName);
//        KuduSession session = client.newSession();
//
//        // insert
//        Insert insert = table.newInsert();
//        PartialRow row = insert.getRow();
//        row.addString("key", "abc");
//        row.addString("value", "abc2");
//        session.apply(insert);
//
//        // query
//        List<String> projectColumns = new ArrayList<>(1);
//        projectColumns.add("value");
//        KuduScanner scanner = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns).build();
//
//        while (scanner.hasMoreRows()) {
//            RowResultIterator results = scanner.nextRows();
//            while (results.hasNext()) {
//                RowResult result = results.next();
//                System.out.println(result.getString(0));
//            }
//        }
    }
}
