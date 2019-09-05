package cn.sunrisecolors.datalake.kudu;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author shaohongliang
 * @since 2019/8/13 18:04
 */
public class KuduType {

    public static Type asType(Object v) throws IllegalArgumentException {
        Type kuduType;
        if (v instanceof String) {
            kuduType = Type.STRING;
        } else if (v instanceof Integer) {
            kuduType = Type.INT64;
        } else if (v instanceof Long) {
            kuduType = Type.INT64;
        } else if (v instanceof Double) {
            kuduType = Type.DOUBLE;
        } else if (v instanceof Float) {
            kuduType = Type.DOUBLE;
        } else if (v instanceof BigDecimal) {
            kuduType = Type.DOUBLE;
        } else if (v instanceof Boolean) {
            kuduType = Type.BOOL;
        } else {
            throw new IllegalArgumentException("Type cannot convert!" + v);
        }
        return kuduType;
    }

    /**
     * 数据类型不一致，隐式转换
     * @return
     */
    public static void handlerKuduRow(Schema schema, Map<String, Object> source, PartialRow row) throws RuntimeException {
        for (String key : source.keySet()) {
            Object value = source.get(key);
            Type type = schema.getColumn(key).getType();
            if(asType(value) != type){
                value = implicitConversion(type, value);
            }
            switch (type){
                case BOOL:
                    row.addBoolean(key, (boolean)value);
                    break;
                case FLOAT:
                    row.addFloat(key, (float)value);
                    break;
                case INT32:
                    row.addInt(key, (int)value);
                    break;
                case INT64:
                    row.addLong(key, (long)value);
                    break;
                case DOUBLE:
                    row.addDouble(key, (double)value);
                    break;
                case STRING:
                    row.addString(key, (String)value);
                    break;
                default:
                    throw new RuntimeException("不支持的数据类型! type:" + type + ", key:" + key+ ", value:" + value);
            }
        }
    }

    private static Object implicitConversion(Type type, Object value) {
        String valueStr = value.toString();
        if (type == Type.STRING) {
            return valueStr;
        } else if (type == Type.INT64) {
            if(NumberUtils.isDigits(valueStr)){
                return Long.parseLong(valueStr);
            }
        }else if (type == Type.INT32) {
            if(NumberUtils.isDigits(valueStr)){
                return Integer.parseInt(valueStr);
            }
        } else if (type == Type.DOUBLE) {
            if(NumberUtils.isCreatable(valueStr)){
                return Double.parseDouble(valueStr);
            }
        } else if (type == Type.BOOL) {
            return Boolean.parseBoolean(valueStr);
        }
        throw new ColumnTypeException("column type is " + type + ", but value is " + value);
    }

}
