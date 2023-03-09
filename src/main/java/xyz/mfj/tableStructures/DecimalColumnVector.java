package xyz.mfj.tableStructures;

import java.math.BigDecimal;
import java.util.Arrays;

public class DecimalColumnVector extends ColumnVector{
    private double[] values;

    public DecimalColumnVector() {}

    @Override
    public BigDecimal getValue(int rowId) {
        return BigDecimal.valueOf(values[rowId]);
    }

    @Override
    public void setValue(int rowId, Object value) {
        values[rowId] = ((BigDecimal)value).doubleValue();
    }

    @Override
    public int size() {
        return values != null ? values.length : 0;
    }

    @Override
    public void resize(int size) {
        if (values == null) {
            values = new double[size];
        }
        else {
            values = Arrays.copyOf(values, size);
        }
    }
}
