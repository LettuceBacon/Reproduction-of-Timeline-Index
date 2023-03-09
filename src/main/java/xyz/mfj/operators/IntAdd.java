package xyz.mfj.operators;

import java.util.function.BiFunction;

public enum IntAdd implements NumericAddOperator{
    INTADD((a, b) -> a + b);
    
    public static IntAdd get() {
        return INTADD;
    }
    
    private BiFunction<Integer, Integer, Integer> oper;
    
    private IntAdd(BiFunction<Integer, Integer, Integer> oper) {
        this.oper =oper;
    }
    
    public BiFunction<Integer, Integer, Integer> getOper() {
        return oper;
    }

    @Override
    public Number add(Number a, Number b) {
        return oper.apply(a.intValue(), b.intValue());
    }
}
