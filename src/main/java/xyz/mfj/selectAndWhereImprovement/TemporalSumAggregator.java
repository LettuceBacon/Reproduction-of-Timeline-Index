package xyz.mfj.selectAndWhereImprovement;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.operators.NumericAddOperator;
import xyz.mfj.operators.NumericMinusOperator;
import xyz.mfj.utils.TypeUtil;

public class TemporalSumAggregator implements Aggregator {
    private static Logger log = LoggerFactory.getLogger(TemporalSumAggregator.class);
    
    private Class<?> type;
    private Number sum;
    private NumericAddOperator add;
    private NumericMinusOperator minus;
    
    public TemporalSumAggregator(Class<?> type) {
        if (!Number.class.isAssignableFrom(type)) {
            log.error("Temporal SUM() can only apply on numeric type like int!");
        }
        this.type = type;
        this.sum = TypeUtil.getZeroOfNumericType(type);
        this.add = TypeUtil.getAddOperOfType(type);
        this.minus = TypeUtil.getMinusOperOfType(type);
    }

    @Override
    public Object aggregate() {
        return sum;
    }

    @Override
    public void collect(Object value, boolean flag) {
        if (flag) {
            sum = add.add(sum, (Number)value);
        }
        else {
            sum = minus.minus(sum, (Number)value);
        }
    }
    
}
