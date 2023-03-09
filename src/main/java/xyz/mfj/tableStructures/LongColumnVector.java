package xyz.mfj.tableStructures;

import java.util.Arrays;

public class LongColumnVector extends ColumnVector{
    private long[] values;

    public LongColumnVector() {}

    @Override
    public Long getValue(int rowId) {
        return values[rowId];
    }

    @Override
    public void setValue(int rowId, Object value) {
        values[rowId] = (long)value;
    }

    @Override
    public int size() {
        return values != null ? values.length : 0;
    }

    @Override
    public void resize(int size) {
        if (values == null) {
            values = new long[size];
        }
        else {
            values = Arrays.copyOf(values, size);
        }
    }
}
