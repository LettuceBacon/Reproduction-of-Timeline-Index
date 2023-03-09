package xyz.mfj.tableStructures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一列数据的存储结构
 */
public abstract class ColumnVector{
    private static Logger log = LoggerFactory.getLogger(ColumnVector.class);
    
    public static ColumnVector newColumnVector(Class<?> clazz) {
        String className = clazz.getName();
        switch (className) {
            case "int":
            case "java.lang.Integer":
                return new IntegerColumnVector();

            case "long":
            case "java.lang.Long":
                return new LongColumnVector();

            case "java.lang.String":
                return new StringColumnVector();

            case "java.sql.Date":
                return new DateColumnVector();
            
            case "java.math.BigDecimal":
                return new DecimalColumnVector();

            case "boolean":
            case "java.lang.Boolean":
                return new BooleanColumnVector();
        
            default:
                log.error("Vector of {} is not supported!\n", className);
                break;
        }
        return null;
        
    }

    /**
     * 让列的容量变成size大小
     * @param size 期望的列容量，以行为单位
     */
    public abstract void resize(int size);
    public abstract Object getValue(int rowId);
    public abstract void setValue(int rowId, Object value);
    public abstract int size();
}
