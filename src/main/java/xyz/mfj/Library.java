package xyz.mfj;

import java.util.HashMap;

import org.apache.commons.lang3.tuple.Pair;

import xyz.mfj.tableStructures.Table;
import xyz.mfj.tableStructures.TimelineIndex;

/**
 * 程序运行时常驻内存的库，用于保存时态表和timeline索引
 */
public class Library {
    /**
     * 表名与表的映射
     */
    private HashMap<String, Table> tables;
    /**
     * 表名、有效时间名与timeline的映射
     */
    private HashMap<Pair<String, String>, TimelineIndex> timelineIndexes;

    private static Library singleton = new Library();
    private Library() {}
    public static Library getInstance() {
        return singleton;
    }

    public void cacheTable(Table table) {
        if (tables == null) {
            tables = new HashMap<>();
        }
        tables.put(table.getTableName(), table);
    }

    public void cacheTimelineIndex(TimelineIndex index) {
        if (timelineIndexes == null) {
            timelineIndexes = new HashMap<>();
        }
        timelineIndexes.put(
            Pair.of(index.getTableName(), index.getApplicationPeriodName()), 
            index
        );
    }

    public Table getTableByName(String tableName) {
        return tables.get(tableName);
    }

    public TimelineIndex getTimelineIndexByName(String tableName, String applicationPeriodName)
    {
        return timelineIndexes.get(Pair.of(tableName, applicationPeriodName));
    }
}
