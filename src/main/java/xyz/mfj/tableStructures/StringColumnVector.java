package xyz.mfj.tableStructures;

import java.util.Arrays;

public class StringColumnVector extends ColumnVector{
    private byte[][] values;

    public StringColumnVector() {}

    @Override
    public String getValue(int rowId) {
        return new String(values[rowId]);
    }

    @Override
    public void setValue(int rowId, Object value) {
        values[rowId] = ((String)value).getBytes();
    }

    @Override
    public int size() {
        return values != null ? values.length : 0;
    }

    @Override
    public void resize(int size) {
        if (values == null) {
            values = new byte[size][];
        }
        else {
            values = Arrays.copyOf(values, size);
        }
    }
}
