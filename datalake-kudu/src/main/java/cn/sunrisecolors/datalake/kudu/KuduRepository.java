package cn.sunrisecolors.datalake.kudu;

import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.kudu.shaded.com.google.common.base.Predicates;
import org.apache.kudu.shaded.com.google.common.cache.CacheBuilder;
import org.apache.kudu.shaded.com.google.common.cache.CacheLoader;
import org.apache.kudu.shaded.com.google.common.cache.LoadingCache;
import org.apache.kudu.shaded.com.google.common.collect.Iterators;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author :hujiansong
 * @date :2019/8/6 15:11
 * @since :1.8
 */
@Slf4j
public class KuduRepository {

    private KuduClient kuduClient;

    private KeyColumns keyColumns;

    // 全局缓存KuduTable对象，避免频繁远程拉取Schema
    private LoadingCache<String, Optional<KuduTable>> kuduTableCache =
            CacheBuilder.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build(
                    new CacheLoader<String, Optional<KuduTable>>() {
                        @Override
                        public Optional<KuduTable> load(String tableName) throws Exception {
                            if (kuduClient.tableExists(tableName)) {
                                return Optional.of(kuduClient.openTable(tableName));
                            } else {
                                return Optional.empty();
                            }
                        }

                    }
            );


    public void refreshTable(String name) {
        kuduTableCache.refresh(name);
    }

    public void createTable(String name, List<ColumnSchema> columnSchemas, String rangeValue) throws KuduException {
        // 追加默认主键
        columnSchemas.add(keyColumns.getId());
        columnSchemas.add(keyColumns.getRange());
        columnSchemas.add(keyColumns.getHash());
        Schema schema = new Schema(columnSchemas);

        // 添加分区定义
        CreateTableOptions options = new CreateTableOptions();
        options.setNumReplicas(1);
        options.addHashPartitions(Collections.singletonList(keyColumns.getHashName()), 16);
        options.setRangePartitionColumns(Collections.singletonList(keyColumns.getRangeName()));

        // 添加数据分区
        PartialRow startKey = new PartialRow(schema);
        startKey.addString(keyColumns.getRangeName(), rangeValue);
        options.addRangePartition(startKey, startKey, RangePartitionBound.INCLUSIVE_BOUND, RangePartitionBound.INCLUSIVE_BOUND);

        kuduClient.createTable(name, new Schema(columnSchemas), options);
        refreshTable(name);
    }

    public void alterTable(KuduTable kuduTable, List<ColumnSchema> columns, String rangeValue) throws KuduException {
        AlterTableOptions ato = new AlterTableOptions();

        boolean hasNewColumn = false;
        boolean hasNewRange = true;
        if(columns.size() > 0){
            for(ColumnSchema columnSchema : columns){
                ato.addColumn(columnSchema);
            }
            hasNewColumn = true;
        }

        List<KuduScanToken> kuduScanTokens = kuduClient.newScanTokenBuilder(kuduTable).build();
        for (KuduScanToken kuduScanToken : kuduScanTokens) {
            Partition partition = kuduScanToken.getTablet().getPartition();
            if (!Iterators.all(partition.getHashBuckets().iterator(), Predicates.equalTo(0))) {
                continue;
            }
            String start = new String(partition.getRangeKeyStart());
            if(start.equals(rangeValue)){
                hasNewRange = false;
                break;
            }
        }

        if(hasNewRange){
            Schema schema = new Schema(kuduTable.getSchema().getPrimaryKeyColumns());
            PartialRow startKey = schema.newPartialRow();
            startKey.addString(keyColumns.getRangeName(), rangeValue);
            PartialRow endKey = schema.newPartialRow();
            endKey.addString(keyColumns.getRangeName(), rangeValue);

            ato.addRangePartition(startKey, endKey, RangePartitionBound.INCLUSIVE_BOUND, RangePartitionBound.INCLUSIVE_BOUND);
        }

        if(hasNewColumn || hasNewRange){
            kuduClient.alterTable(kuduTable.getName(), ato);
            refreshTable(kuduTable.getName());
        }
    }

    public KuduTable getTable(String name) {
        try {
            Optional<KuduTable> kuduTable = kuduTableCache.get(name);
            if (kuduTable.isPresent()) {
                return kuduTable.get();
            } else {
                return null;
            }
        } catch (ExecutionException e) {
            e.printStackTrace();
            return null;
        }
    }

    // 判断是否存在
    public boolean exist(String table, String id) throws KuduException {
        KuduPredicate idEqualPredict = KuduPredicate.newComparisonPredicate(keyColumns.getId(), KuduPredicate.ComparisonOp.EQUAL, id);
        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(getTable(table))
                .setProjectedColumnNames(Arrays.asList(keyColumns.getIdName()));
        scannerBuilder.addPredicate(idEqualPredict);
        KuduScanner scanner = scannerBuilder.build();
        while (scanner.hasMoreRows()) {
            if (scanner.nextRows().hasNext()) {
                return true;
            }
        }
        return false;
    }

    public OperationResponse insert(String name, String id, Map<String, Object> source) throws KuduException, RuntimeException {
        KuduTable table = getTable(name);
        Insert insert = table.newInsert();
        return insertOrUpdate(insert, table.getSchema(), id, source);
    }

    public OperationResponse upsert(String name, String id, Map<String, Object> source) throws KuduException, RuntimeException {
        KuduTable table = getTable(name);
        Upsert upsert = table.newUpsert();
        return insertOrUpdate(upsert, table.getSchema(), id, source);
    }

    private OperationResponse insertOrUpdate(Operation operation, Schema schema, String id, Map<String, Object> source) throws KuduException {
        KuduSession session = kuduClient.newSession();
        PartialRow row = operation.getRow();
        row.addString(keyColumns.getIdName(), id);
        KuduType.handlerKuduRow(schema, source, row);
        OperationResponse response = session.apply(operation);
        session.close();
        return response;
    }

}
