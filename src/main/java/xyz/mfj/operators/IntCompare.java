package xyz.mfj.operators;

import java.util.Comparator;

public enum IntCompare implements CompareOperator{
    INTCOMPARE((a, b) -> ((Integer)a).compareTo((Integer)b));
    
    public static IntCompare get() {
        return INTCOMPARE;
    }
    
    private Comparator<Object> oper;
    
    private IntCompare(Comparator<Object> oper) {
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
