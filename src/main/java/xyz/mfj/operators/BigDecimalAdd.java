package xyz.mfj.operators;

import java.math.BigDecimal;
import java.util.function.BiFunction;

public enum BigDecimalAdd implements NumericAddOperator{
    BIGDECIMALADD((a, b) -> a.add(b));
    
    public static BigDecimalAdd get() {
        return BIGDECIMALADD;
    }
    
    private BiFunction<BigDecimal, BigDecimal, BigDecimal> oper;
    
    private BigDecimalAdd(BiFunction<BigDecimal, BigDecimal, BigDecimal> oper) {
        this.oper =oper;
    }
    
    public BiFunction<BigDecimal, BigDecimal, BigDecimal> getOper() {
        return oper;
    }

    @Override
    public Number add(Number a, Number b) {
        return oper.apply((BigDecimal)a, (BigDecimal)b);
    }
}
