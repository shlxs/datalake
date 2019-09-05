package cn.sunrisecolors.datalake.kudu;

import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowError;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 把kudu当作类ES一样的文档数据库，支持以下特性
 * 1、自动创建表
 * 2、自动创建列
 * 3、字段允许隐式转换
 * 4、表名、字段名不区分大小写（统一转为小写处理）
 * 5、每条记录拥有支持唯一ID
 *
 * @author :hujiansong
 * @date :2019/8/6 17:40
 * @since :1.8
 */
@Slf4j
public class KuduDocumentService {
    private KuduRepository kuduDao;

    private KeyColumns keyColumns;

    public boolean exist(String table, String id) throws KuduException {
        return kuduDao.exist(table, id);
    }

    public void insert(String table, String id, Map<String, Object> source) throws KuduException {
        createOrAlterTable(table, source);
        OperationResponse response = kuduDao.insert(table, id, source);
        doResponse(response);
    }

    public void upsert(String table, String id, Map<String, Object> source) throws KuduException {
        createOrAlterTable(table, source);
        OperationResponse response = kuduDao.upsert(table, id, source);
        doResponse(response);
    }

    private void doResponse(OperationResponse response) {
        if(response.hasRowError()){
            RowError rowError = response.getRowError();
            log.error(rowError.toString());
            throw new RuntimeException(rowError.toString());
        }
    }

    /**
     * @param table
     * @param source
     * @return
     * @throws KuduException
     */
    private void createOrAlterTable(String table, Map<String, Object> source) throws KuduException {
        // 校验分区是否有值
        String rangeColumn = keyColumns.getRange().getName();
        String rangeValue = (String)source.get(rangeColumn);
        if(rangeValue == null){
            throw new IllegalArgumentException(String.format("range column {0} is null", rangeColumn));
        }

        KuduTable kuduTable = kuduDao.getTable(table);
        if( kuduTable == null ){
            // 表不存在
            List<ColumnSchema> columnSchemas = toKuduColumnSchema(source);
            kuduDao.createTable(table, columnSchemas, rangeValue);
        } else {
            // 忽略存在的列
            Map<String, Object> newSource = removeExistColumns(kuduTable, source);
            List<ColumnSchema> columnSchemas = toKuduColumnSchema(newSource);
            kuduDao.alterTable(kuduTable, columnSchemas, rangeValue);
        }
    }

    /**
     * 移除已经存在的列，剩余的就是需要新增的列
     * @param kuduTable
     * @param source
     * @return
     */
    private Map<String, Object> removeExistColumns(KuduTable kuduTable, Map<String, Object> source) {
        Map<String, Object> newSource = new HashMap<>(source);
        List<ColumnSchema> columnSchemas = kuduTable.getSchema().getColumns();
        for (ColumnSchema columnSchema : columnSchemas){
            if(source.containsKey(columnSchema.getName())){
                newSource.remove(columnSchema.getName());
            }
        }
        return newSource;
    }

    private List<ColumnSchema> toKuduColumnSchema(Map<String, Object> source) {
        List<ColumnSchema> columnSchemas = new ArrayList<>();
        for(String key : source.keySet()){
            String columnName = key.toLowerCase();
            Object columnValue = source.get(key);
            if (columnValue != null) {
                Type columnType = KuduType.asType(columnValue);
                columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder(columnName, columnType).nullable(true).build());
            }
        }
        return columnSchemas;
    }

}
