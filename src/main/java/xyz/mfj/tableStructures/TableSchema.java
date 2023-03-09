package xyz.mfj.tableStructures;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

/**
 * 表模式
 */
public class TableSchema {
    private List<String> columnNames;
    private List<Class<?>> columnTypes;

    public TableSchema(List<Pair<String, Class<?>>> columnList) {
        this.columnNames = new ArrayList<>(columnList.size());
        this.columnTypes = new ArrayList<>(columnList.size());
        for (Pair<String,Class<?>> col : columnList) {
            columnNames.add(col.getLeft());
            columnTypes.add(col.getRight());
        }
    }
    
    public Class<?> getTypeByName(String columnName) {
        return columnTypes.get(columnNames.indexOf(columnName));
    }

    public List<Class<?>> getColumnTypes() {
        return columnTypes;
    }

    public int getColumnSize() {
        return columnTypes.size();
    }

    public int getColumnIndex(String columnName) {
        return columnNames.indexOf(columnName);
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

}
