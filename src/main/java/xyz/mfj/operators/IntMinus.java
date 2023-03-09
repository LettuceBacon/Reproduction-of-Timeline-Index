package xyz.mfj.operators;

import java.util.function.BiFunction;

public enum IntMinus implements NumericMinusOperator{
    INTMINUS((a, b) -> a - b);
    
    public static IntMinus get() {
        return INTMINUS;
    }
    
    private BiFunction<Integer, Integer, Integer> oper;
    
    private IntMinus(BiFunction<Integer, Integer, Integer> oper) {
        this.oper =oper;
    }
    
    public BiFunction<Integer, Integer, Integer> getOper() {
        return oper;
    }

    @Override
    public Number minus(Number a, Number b) {
        return oper.apply(a.intValue(), b.intValue());
    }
}
