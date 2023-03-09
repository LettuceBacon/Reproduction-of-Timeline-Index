package xyz.mfj.operators;

import java.util.Comparator;

public enum LongCompare implements CompareOperator{
    LONGCOMPARE((a, b) -> ((Long)a).compareTo((Long)b));
    
    public static LongCompare get() {
        return LONGCOMPARE;
    }
    
    private Comparator<Object> oper;
    
    private LongCompare(Comparator<Object> oper) {
        this.oper =oper;
    }
    
    @Override
    public Comparator<Object> getOper() {
        return oper;
    }

    @Override
    public int compare(Object a, Object b) {
        return oper.compare(a, b);
    }

}
