package xyz.mfj.utils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.operators.BigDecimalAdd;
import xyz.mfj.operators.BigDecimalCompare;
import xyz.mfj.operators.BigDecimalMinus;
import xyz.mfj.operators.IntAdd;
import xyz.mfj.operators.IntCompare;
import xyz.mfj.operators.IntMinus;
import xyz.mfj.operators.LongAdd;
import xyz.mfj.operators.LongCompare;
import xyz.mfj.operators.LongMinus;
import xyz.mfj.operators.NumericAddOperator;
import xyz.mfj.operators.CompareOperator;
import xyz.mfj.operators.DateCompare;
import xyz.mfj.operators.NumericMinusOperator;

public class TypeUtil {
    private static Logger log = LoggerFactory.getLogger(TypeUtil.class);
    
    public static Object castStringTo(Class<?> clazz, String str) {
        String className = clazz.getName();
        switch (className) {
            case "int":
            case "java.lang.Integer":
                return Integer.valueOf(str);

            case "long":
            case "java.lang.Long":
                return Long.valueOf(str);

            case "java.lang.String":
                return str;

            case "java.sql.Date":
                return Date.valueOf(str);
            case "java.sql.Timestamp":
                return Timestamp.valueOf(str);
            case "java.util.Date":
                log.error("Use concrete date type like java.sql.Date instead of java.util.Date!");
                break;
            
            case "java.math.BigDecimal":
                return new BigDecimal(str);

            case "boolean":
            case "java.lang.Boolean":
                return Boolean.valueOf(str);
        
            default:
                log.error("String cannot be cast to {}\n", className);
                break;
        }
        return null;
    }
    
    public static Number getZeroOfNumericType(Class<?> type) {
        String typeName = type.getName();
        switch (typeName) {
            case "int":
            case "java.lang.Integer":
                return 0;

            case "long":
            case "java.lang.Long":
                return 0L;
            
            case "java.math.BigDecimal":
                return BigDecimal.ZERO;
        
            default:
                log.error("{} has no zero\n", typeName);
                break;
        }
        return null;
        
    }
    
    public static NumericAddOperator getAddOperOfType(Class<?> type) {
        String typeName = type.getName();
        switch (typeName) {
            case "int":
            case "java.lang.Integer":
                return IntAdd.INTADD;

            case "long":
            case "java.lang.Long":
                return LongAdd.LONGADD;
            
            case "java.math.BigDecimal":
                return BigDecimalAdd.BIGDECIMALADD;
        
            default:
                log.error("No addition for type {}\n", typeName);
                break;
        }
        return null;
        
    } 
    
    public static NumericMinusOperator getMinusOperOfType(Class<?> type) {
        String typeName = type.getName();
        switch (typeName) {
            case "int":
            case "java.lang.Integer":
                return IntMinus.INTMINUS;

            case "long":
            case "java.lang.Long":
                return LongMinus.LONGMINUS;
            
            case "java.math.BigDecimal":
                return BigDecimalMinus.BIGDECIMALMINUS;
        
            default:
                log.error("No substraction for type {}\n", typeName);
                break;
        }
        return null;
        
    }
    
    public static Object getMinOfType(Class<?> type) {
        String typeName = type.getName();
        switch (typeName) {
            case "int":
            case "java.lang.Integer":
                return Integer.MIN_VALUE;

            case "long":
            case "java.lang.Long":
                return Long.MIN_VALUE;
            
            case "java.math.BigDecimal":
                return BigDecimal.valueOf(Double.MIN_VALUE);
                
            case "java.sql.Timestamp":
                return new Timestamp(0L);
                
            case "java.sql.Date":
                return new Date(0L);
        
            default:
                log.error("{} doesn't have min value\n", typeName);
                break;
        }
        return null;
        
    }
    
    public static Object getMaxOfType(Class<?> type) {
        String typeName = type.getName();
        switch (typeName) {
            case "int":
            case "java.lang.Integer":
                return Integer.MAX_VALUE;

            case "long":
            case "java.lang.Long":
                return Long.MAX_VALUE;
            
            case "java.math.BigDecimal":
                return BigDecimal.valueOf(Double.MAX_VALUE);
                
            case "java.sql.Timestamp":
                return new Timestamp(253402271999999L);
            
            case "java.sql.Date":
                return new Date(253402185600000L);
        
            default:
                log.error("{} doesn't have max value\n", typeName);
                break;
        }
        return null;
        
    }
    
    public static CompareOperator getCmprOperOfType(Class<?> type) {
        String typeName = type.getName();
        switch (typeName) {
            case "int":
            case "java.lang.Integer":
                return IntCompare.INTCOMPARE;

            case "long":
            case "java.lang.Long":
                return LongCompare.LONGCOMPARE;
            
            case "java.math.BigDecimal":
                return BigDecimalCompare.BIGDECIMALCOMPARE;

            case "java.sql.Date":
                return DateCompare.DATECOMPARE;
        
            default:
                log.error("{} doesn't have compare operator\n", typeName);
                break;
        }
        return null;
        
    }
    
}
