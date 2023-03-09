package xyz.mfj.tableStructures;

import java.util.Arrays;

public class BooleanColumnVector extends ColumnVector{
    private boolean[] values;

    public BooleanColumnVector() {}

    @Override
    public Boolean getValue(int rowId) {
        return values[rowId];
    }

    @Override
    public void setValue(int rowId, Object value) {
        values[rowId] = (boolean)value;
    }

    @Override
    public int size() {
        return values != null ? values.length : 0;
    }

    @Override
    public void resize(int size) {
        if (values == null) {
            values = new boolean[size];
        }
        else {
            values = Arrays.copyOf(values, size);
        }
    }

    
}
