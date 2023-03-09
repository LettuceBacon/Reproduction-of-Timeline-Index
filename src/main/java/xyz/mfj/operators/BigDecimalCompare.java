package xyz.mfj.operators;

import java.math.BigDecimal;
import java.util.Comparator;

public enum BigDecimalCompare implements CompareOperator{
    BIGDECIMALCOMPARE((a, b) -> ((BigDecimal)a).compareTo((BigDecimal)b));
    
    public static BigDecimalCompare get() {
        return BIGDECIMALCOMPARE;
    }
    
    private Comparator<Object> oper;
    
    private BigDecimalCompare(Comparator<Object> oper) {
        this.oper =oper;
    }
    
    public Comparator<Object> getOper() {
        return oper;
    }

    @Override
    public int compare(Object a, Object b) {
        return oper.compare((BigDecimal)a, (BigDecimal)b);
    }
}
