package cn.sunrisecolors.datalake.kudu;

import lombok.Data;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;

/**
 * @author shaohongliang
 * @since 2019/9/2 14:46
 */
@Data
public final class KeyColumns {
    private ColumnSchema id = new ColumnSchema.ColumnSchemaBuilder("_id", Type.STRING).key(true).build();

    private ColumnSchema range = new ColumnSchema.ColumnSchemaBuilder("date", Type.STRING).key(true).build();

    private ColumnSchema hash = new ColumnSchema.ColumnSchemaBuilder("app_id", Type.STRING).key(true).build();

    public String getHashName(){
        return hash.getName();
    }

    public String getRangeName(){
        return range.getName();
    }

    public String getIdName(){
        return id.getName();
    }

}
