package xyz.mfj.operators;

import java.util.function.BiFunction;

public enum LongMinus implements NumericMinusOperator{
    LONGMINUS((a, b) -> a - b);
    
    public static LongMinus get() {
        return LONGMINUS;
    }
    
    private BiFunction<Long, Long, Long> oper;
    
    private LongMinus(BiFunction<Long, Long, Long> oper) {
        this.oper =oper;
    }
    
    public BiFunction<Long, Long, Long> getOper() {
        return oper;
    }

    @Override
    public Number minus(Number a, Number b) {
        return oper.apply(a.longValue(), b.longValue());
    }
}
