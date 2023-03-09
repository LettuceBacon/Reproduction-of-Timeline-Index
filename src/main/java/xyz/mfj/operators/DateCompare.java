package xyz.mfj.operators;

import java.util.Comparator;
import java.sql.Date;

public enum DateCompare implements CompareOperator{
    DATECOMPARE((a, b) -> ((Date)a).compareTo((Date)b));
    
    public static DateCompare get() {
        return DATECOMPARE;
    }
    
    private Comparator<Object> oper;
    
    private DateCompare(Comparator<Object> oper) {
        this.oper =oper;
    }
    
    public Comparator<Object> getOper() {
        return oper;
    }

    @Override
    public int compare(Object a, Object b) {
        return oper.compare(a, b);
    }
}
