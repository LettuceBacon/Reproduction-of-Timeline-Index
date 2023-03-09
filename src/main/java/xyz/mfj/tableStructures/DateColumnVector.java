package xyz.mfj.tableStructures;

import java.util.Arrays;
import java.sql.Date;

public class DateColumnVector extends ColumnVector{
    private long[] values;

    public DateColumnVector() {}

    @Override
    public Date getValue(int rowId) {
        return new Date(values[rowId]);
    }

    @Override
    public void setValue(int rowId, Object value) {
        values[rowId] = ((Date)value).getTime();
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
