package cn.sunrisecolors.datalake.kudeException;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author :hujiansong
 * @date :2019/9/5 9:51
 * @since :1.8
 */
public class KuduCreateUnitTest {

    KuduClient client;
    KuduClient clientEs;

    @Before
    public void buildClient() {
        client = new KuduClient.KuduClientBuilder("hadoop-80130").build();
        clientEs = new KuduClient.KuduClientBuilder("es").build();
    }

    /**
     * throws NonRecoverableException: Table test-A already exists with id d7520e2148484a92be3fd4232cc864ed table已经存在
     * throws NonRecoverableException: Couldn't resolve this master's address hadoop-801300:7051]   master地址不正确
     */
    @Test
    public void tableExist() {
        ColumnSchema id = new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).key(true).nullable(false).build();

        Schema schema = new Schema(Arrays.asList(id));

        CreateTableOptions cto = new CreateTableOptions();
        cto.addHashPartitions(Arrays.asList("id"), 16);
        try {
            client.createTable("$#%*&!^$#@table 1#$", schema, cto);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void invalidColumn() {
        List<ColumnSchema> cList = new ArrayList<>();
        ColumnSchema id = new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).key(true).nullable(false).build();
        cList.add(id);
        ColumnSchema val1 = new ColumnSchema.ColumnSchemaBuilder("$#%*&!^$#@c1 1#$", Type.STRING).key(false).nullable(false).build();
        cList.add(val1);
        Schema schema = new Schema(cList);

        CreateTableOptions cto = new CreateTableOptions();
        cto.addHashPartitions(Arrays.asList("id"), 16);
        try {
            client.createTable("invalidColumnTest1", schema, cto);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    /**
     * throws NonRecoverableException: number of columns 401 is greater than the permitted maximum 300
     */
    @Test
    public void limit300Column() {
        List<ColumnSchema> cList = new ArrayList<>();
        ColumnSchema id = new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).key(true).nullable(false).build();
        cList.add(id);
        for (int i = 0; i < 400; i++) {
            ColumnSchema val1 = new ColumnSchema.ColumnSchemaBuilder("c" + i, Type.STRING).key(false).nullable(false).build();
            cList.add(val1);
        }
        Schema schema = new Schema(cList);

        CreateTableOptions cto = new CreateTableOptions();
        cto.addHashPartitions(Arrays.asList("id"), 16);
        try {
            client.createTable("limit300ColumnTest", schema, cto);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }


    /**
     * NonRecoverableException: Couldn't find a valid master in (hadoop-801300:7051)
     */
    @Test
    public void connectFail() {
        // 正确的KuduClient hadoop-801300
        client = new KuduClient.KuduClientBuilder("hadoop-801300").build();
        try {
            client.openTable("tet");
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }


    /**
     * throws NonRecoverableException: can not complete before timeout: KuduRpc(method=ListTables, tablet=null, attempt=19, DeadlineTracker(timeout=30000, elapsed=28618), Traces: [0ms] querying master, [82ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [1110ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [1114ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [1120ms] querying master, [1120ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [2122ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [2123ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [2140ms] querying master, [2141ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [3136ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [3136ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [3140ms] querying master, [3140ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [4137ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [4138ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [4160ms] querying master, [4160ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [5163ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [5164ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [5200ms] querying master, [5200ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [6203ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [6204ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [6220ms] querying master, [6221ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [7222ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [7222ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [7320ms] querying master, [7320ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [8315ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [8316ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [8400ms] querying master, [8400ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [9402ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [9404ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [9861ms] querying master, [9861ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [10860ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [10862ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [11500ms] querying master, [11501ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [12497ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [12498ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [12561ms] querying master, [12561ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [13559ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [13561ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [15660ms] querying master, [15661ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [16664ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [16666ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [17000ms] querying master, [17001ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [18000ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [18001ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [19160ms] querying master, [19161ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [20160ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [20160ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [20981ms] querying master, [20981ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [21978ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [21979ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [22321ms] querying master, [22321ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [23323ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [23324ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, [24101ms] querying master, [24101ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [25104ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Failed to connect to peer master-es:7051(es:7051): Connection refused: no further information: es/10.191.72.200:7051, [25105ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Failed to connect to peer master-es:7051(es:7051): Connection refused: no further information: es/10.191.72.200:7051, [27621ms] querying master, [27621ms] Sub rpc: ConnectToMaster sending RPC to server master-es:7051, [28615ms] Sub rpc: ConnectToMaster received from server master-es:7051 response Network error: Connection refused: no further information: es/10.191.72.200:7051, [28617ms] delaying RPC due to Service unavailable: Master config (es:7051) has no leader. Exceptions received: org.apache.kudu.client.RecoverableException: Connection refused: no further information: es/10.191.72.200:7051, Deferred@1175308394(state=PENDING, result=null, callback=(continuation of Deferred@1401837150 after retry RPC after error@439260963) -> (continuation of Deferred@1666836829 after retry RPC after error@1839078046) -> (continuation of Deferred@1156296283 after retry RPC after error@1244420462) -> (continuation of Deferred@34204233 after retry RPC after error@214832544) -> (continuation of Deferred@32066550 after retry RPC after error@254714979) -> (continuation of Deferred@1266110265 after retry RPC after error@1169209855) -> (continuation of Deferred@1686793746 after retry RPC after error@1783471403) -> (continuation of Deferred@61046942 after retry RPC after error@224689986) -> (continuation of Deferred@114885788 after retry RPC after error@1746932020) -> (continuation of Deferred@577037158 after retry RPC after error@1734767086) -> (continuation of Deferred@97952864 after retry RPC after error@1087648286) -> (continuation of Deferred@1127538802 after retry RPC after error@104234704) -> (continuation of Deferred@1087652301 after retry RPC after error@97963795) -> (continuation of Deferred@172639490 after retry RPC after error@1330172559) -> (continuation of Deferred@632775721 after retry RPC after error@1622537819) -> (continuation of Deferred@1117686437 after retry RPC after error@127669298) -> (continuation of Deferred@253906330 after retry RPC after error@784323253) -> (continuation of Deferred@1809924863 after retry RPC after error@1612004701), errback=(continuation of Deferred@1401837150 after retry RPC after error@439260963) -> (continuation of Deferred@1666836829 after retry RPC after error@1839078046) -> (continuation of Deferred@1156296283 after retry RPC after error@1244420462) -> (continuation of Deferred@34204233 after retry RPC after error@214832544) -> (continuation of Deferred@32066550 after retry RPC after error@254714979) -> (continuation of Deferred@1266110265 after retry RPC after error@1169209855) -> (continuation of Deferred@1686793746 after retry RPC after error@1783471403) -> (continuation of Deferred@61046942 after retry RPC after error@224689986) -> (continuation of Deferred@114885788 after retry RPC after error@1746932020) -> (continuation of Deferred@577037158 after retry RPC after error@1734767086) -> (continuation of Deferred@97952864 after retry RPC after error@1087648286) -> (continuation of Deferred@1127538802 after retry RPC after error@104234704) -> (continuation of Deferred@1087652301 after retry RPC after error@97963795) -> (continuation of Deferred@172639490 after retry RPC after error@1330172559) -> (continuation of Deferred@632775721 after retry RPC after error@1622537819) -> (continuation of Deferred@1117686437 after retry RPC after error@127669298) -> (continuation of Deferred@253906330 after retry RPC after error@784323253) -> (continuation of Deferred@1809924863 after retry RPC after error@1612004701)))
     */
    @Test
    public void connectTimeOut() {
        try {
            clientEs.getTablesList();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }


    /**
     * throws NonRecoverableException: range partition lower bound must be less than the upper bound: "2019-09-05" <= VALUES <
     */
    @Test
    public void rangePartitionExist() {
        ColumnSchema id = new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).key(true).nullable(false).build();
        ColumnSchema date = new ColumnSchema.ColumnSchemaBuilder("item_date", Type.STRING).key(true).nullable(false).build();

        Schema schema = new Schema(Arrays.asList(id, date));

        CreateTableOptions cto = new CreateTableOptions();
        cto.addHashPartitions(Arrays.asList("id"), 16);
        cto.setRangePartitionColumns(Arrays.asList("item_date"));
        PartialRow start = new PartialRow(schema);
        start.addString("item_date", "2019-09-05");

        PartialRow end = new PartialRow(schema);
        end.addString("item_date", "2019-09-05");
        cto.addRangePartition(start, end, RangePartitionBound.INCLUSIVE_BOUND, RangePartitionBound.INCLUSIVE_BOUND);
        //     cto.addRangePartition(start, start, RangePartitionBound.INCLUSIVE_BOUND, RangePartitionBound.INCLUSIVE_BOUND);
        try {
            client.createTable("rangePartitionTest1", schema, cto);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}
