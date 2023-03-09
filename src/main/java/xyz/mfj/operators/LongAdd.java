package xyz.mfj.operators;

import java.util.function.BiFunction;

public enum LongAdd implements NumericAddOperator{
    LONGADD((a, b) -> a + b);
    
    public static LongAdd get() {
        return LONGADD;
    }
    
    private BiFunction<Long, Long, Long> oper;
    
    private LongAdd(BiFunction<Long, Long, Long> oper) {
        this.oper =oper;
    }
    
    public BiFunction<Long, Long, Long> getOper() {
        return oper;
    }

    @Override
    public Number add(Number a, Number b) {
        return oper.apply(a.longValue(), b.longValue());
    }
}
