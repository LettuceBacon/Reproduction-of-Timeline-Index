package xyz.mfj.tableStructures;

import java.util.Arrays;

public class IntegerColumnVector extends ColumnVector{
    private int[] values;

    public IntegerColumnVector() {}

    @Override
    public Integer getValue(int rowId) {
        return values[rowId];
    }

    @Override
    public void setValue(int rowId, Object value) {
        values[rowId] = (int)value;
    }

    @Override
    public int size() {
        return values != null ? values.length : 0;
    }

    @Override
    public void resize(int size) {
        if (values == null) {
            values = new int[size];
        }
        else {
            values = Arrays.copyOf(values, size);
        }
    }
    
}
