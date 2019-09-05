package cn.sunrisecolors.datalake.kudu;

/**
 * @author shaohongliang
 * @since 2019/8/13 17:18
 */
public class ColumnTypeException extends RuntimeException{
    public ColumnTypeException() {
    }

    public ColumnTypeException(String message) {
        super(message);
    }

    public ColumnTypeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ColumnTypeException(Throwable cause) {
        super(cause);
    }

    public ColumnTypeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
