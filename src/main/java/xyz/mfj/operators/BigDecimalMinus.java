package xyz.mfj.operators;

import java.math.BigDecimal;
import java.util.function.BiFunction;

public enum BigDecimalMinus implements NumericMinusOperator{
    BIGDECIMALMINUS((a, b) -> a.subtract(b));
    
    public static BigDecimalMinus get() {
        return BIGDECIMALMINUS;
    }
    
    private BiFunction<BigDecimal, BigDecimal, BigDecimal> oper;
    
    private BigDecimalMinus(BiFunction<BigDecimal, BigDecimal, BigDecimal> oper) {
        this.oper =oper;
    }
    
    public BiFunction<BigDecimal, BigDecimal, BigDecimal> getOper() {
        return oper;
    }

    @Override
    public Number minus(Number a, Number b) {
        return oper.apply((BigDecimal)a, (BigDecimal)b);
    }
}
