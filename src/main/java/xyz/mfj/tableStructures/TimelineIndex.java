package xyz.mfj.tableStructures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.operators.DateCompare;

public class TimelineIndex {
    private static Logger log = LoggerFactory.getLogger(TimelineIndex.class);
    
    private static final ColumnStoreTable newEventList() {
        String tableName = "eventList";
        List<Pair<String, Class<?>>> columnList = new ArrayList<>();
        columnList.add(Pair.of("rowId", int.class));
        columnList.add(Pair.of("flag", boolean.class));
        ColumnStoreTable eventList = new ColumnStoreTable(
            new TableSchema(columnList), 
            null,
            tableName);

        return eventList;
    }

    private static final ColumnStoreTable newVersionMap(Class<?> versionType) {
        String tableName = "versionMap";
        List<Pair<String, Class<?>>> columnList = new ArrayList<>();
        columnList.add(Pair.of("version", versionType));
        columnList.add(Pair.of("eventId", int.class));
        ColumnStoreTable eventList = new ColumnStoreTable(
            new TableSchema(columnList), 
            null, 
            tableName);
        return eventList;
    }

    private ColumnStoreTable versionMap;
    private ColumnStoreTable eventList;
    // TODO：checkpoints结构
    private String tableName;
    private String applicationPeriodName;

    public TimelineIndex() {}

    /**
     * 根据一个时态表和表上的一个有效时间构造timeline索引
     * @param table 时态表
     * @param applicationPeriodName 有效时间名
     */
    public TimelineIndex(ColumnStoreTable table, String applicationPeriodName) {
        if (!table.isTemporal()) {
            log.error("Table is non-temporal!");
            System.exit(1);
        }
        if (!table.getApplicationPeriod().containsPeriod(applicationPeriodName)) {
            log.error("Table doesn't contains {}!\n", applicationPeriodName);
            System.exit(1);
        }

        long stime = System.currentTimeMillis();

        this.tableName = table.getTableName();
        this.applicationPeriodName = applicationPeriodName;
        
        // 将有效时间起止时间合并并排序
        TableSchema tableSchema = table.getTableSchema();
        ApplicationPeriod appPeriod = table.getApplicationPeriod();
        int rowNumber = table.getRowSize();
        Date[] intermediateDates = new Date[rowNumber * 2];
        int startColumnIndex = tableSchema.getColumnIndex(appPeriod.getStartName(applicationPeriodName));
        int endColumnIndex = tableSchema.getColumnIndex(appPeriod.getEndName(applicationPeriodName));
        DateColumnVector startColumn = (DateColumnVector)table.getColumnAt(startColumnIndex);
        DateColumnVector endColumn = (DateColumnVector)table.getColumnAt(endColumnIndex);        
        for (int rowIndex = 0; rowIndex < rowNumber; rowIndex++) {
            intermediateDates[rowIndex] = startColumn.getValue(rowIndex);
        }
        for (int rowIndex = 0; rowIndex < rowNumber; rowIndex++) {
            intermediateDates[rowIndex + rowNumber] = endColumn.getValue(rowIndex);
        }
        // Arrays.sort(intermediateDates, (a, b) -> a.compareTo(b));
        Arrays.sort(intermediateDates, DateCompare.DATECOMPARE.getOper());

        // 构造versionMap，每个不同的时间版本都插入到versionMap中
        // intermediateDates[101，101，102，103，103，103] -> versionMap{{101:0}, {102:2}, {103:3}}
        int eventCount = 0;
        TreeMap<Date, Integer> intermediateVersionMap = new TreeMap<>();
        for (Date date : intermediateDates) {
            if (!intermediateVersionMap.containsKey(date)) {
                intermediateVersionMap.put(date, eventCount);
            }
            eventCount++;
        }

        // 构造eventList
        // 每读取一行，先在versionMap中找到eventId，
        // 然后在eventList的eventId行插入行号和起止时间标记，
        // 最后递增versionMap中eventId。
        this.eventList = newEventList();
        int eventListIndex = 0;
        for (int rowIndex = 0; rowIndex < rowNumber; rowIndex++) {
            Date startTime = startColumn.getValue(rowIndex);
            eventListIndex = intermediateVersionMap.get(startTime);
            eventList.addRowAt(eventListIndex, new RowVector(rowIndex, true));
            intermediateVersionMap.put(startTime, eventListIndex + 1);

            Date endTime = endColumn.getValue(rowIndex);
            eventListIndex = intermediateVersionMap.get(endTime);
            eventList.addRowAt(eventListIndex, new RowVector(rowIndex, false));
            intermediateVersionMap.put(endTime, eventListIndex + 1);
        }

        this.versionMap = newVersionMap(
            tableSchema.getTypeByName(appPeriod.getStartName(applicationPeriodName))
        );
        int versionMapIndex = 0;
        for (Entry<Date, Integer> versionMapPair : intermediateVersionMap.entrySet()) {
            versionMap.addRowAt(
                versionMapIndex, 
                new RowVector(versionMapPair.getKey(), versionMapPair.getValue())
            );
            versionMapIndex++;
        }

        long etime = System.currentTimeMillis();
        log.info("{} index construction time {} ms\n", applicationPeriodName, etime - stime);
        
    }

    public String getTableName() {
        return this.tableName;
    }

    public String getApplicationPeriodName() {
        return this.applicationPeriodName;
    }

    public ColumnStoreTable getVersionMap() {
        return this.versionMap;
    }

    public ColumnStoreTable getEventList() {
        return this.eventList;
    }
    
    public Date getMinVersion() {
        return (Date)versionMap.getValueAt(0, 0);
    }
    
    public Date getMaxVersion() {
        return (Date)versionMap.getValueAt(0, versionMap.getRowSize() - 1);
    }
    
    /**
     * 在一个时间版本上的versionMap行号和eventList行号范围。
     */
    public static class RowIndexAtVersion {
        private int vi;
        private int eiStart;
        private int eiEnd;
        
        public RowIndexAtVersion(int vi, int eiStart, int eiEnd) {
            this.vi = vi;
            this.eiStart = eiStart;
            this.eiEnd = eiEnd;
        }
        
        /**
         * 获取VersionMap的行号，与TimelineIterator配合使用
         * @return VersionMap的行号
         */
        public int getVi() {
            return vi;
        }
        
        /**
         * 获取当前时间版本对应的第一个事件在EventList中的行号
         * @return 当前时间版本对应的第一个事件在EventList中的行号
         */
        public int getEiStart() {
            return eiStart;
        }
        
        /**
         * 获取当前时间版本对应的最后一个事件在EventList中的行号加一
         * @return 当前时间版本对应的最后一个事件在EventList中的行号加一
         */
        public int getEiEnd() {
            return eiEnd;
        }
    }
    
    /**
     * 索引迭代器。用于读取从一个时间版本到另一个时间版本之间的时间版本及有关的事件。
     * 例如：
     * VersionMap:{Version:[101, 102, 103],
     *              EventId:[1, 2, 5]},
     * EventList:{RowId:[1, 2, 1, 3, 4],
     *              Flag:[1, 1, 0, 1, 1]}
     * 假如从101读到103（不包含），迭代会生成{vi:0, eiStart:0, eiEnd:1}和{vi:1, eiStart:1, eiEnd:2}
     */
    private class TimelineIterator implements Iterator<RowIndexAtVersion> {
        private int vi;
        private DateColumnVector version;
        private IntegerColumnVector eventId;
        private int versionMapRowNumber;
        private Date toTime; // 迭代器会读到该时间之前的一个时间版本
        
        public TimelineIterator(ColumnStoreTable versionMap, int fromVi, Date toTime) 
        {
            this.version = (DateColumnVector)versionMap.getColumnAt(0);
            this.eventId = (IntegerColumnVector)versionMap.getColumnAt(1);
            this.versionMapRowNumber = versionMap.getRowSize();
            this.vi = fromVi;
            this.toTime = toTime;
        }

        @Override
        public boolean hasNext() {
            if (vi < versionMapRowNumber
                && version.getValue(vi).compareTo(toTime) < 0)
            {
                return true;
            }
            else 
            {
                return false;
            }
        }

        @Override
        public RowIndexAtVersion next() {
            int eiStart = vi > 0 ? eventId.getValue(vi - 1) : 0;
            int eiEnd = eventId.getValue(vi);
            RowIndexAtVersion rowIndexAtVersion = new RowIndexAtVersion(vi, eiStart, eiEnd);
            vi++;
            return rowIndexAtVersion;
        }
        
    }
    
    public Iterator<RowIndexAtVersion> iterator(int fromVi, Date toTime) {
        return new TimelineIterator(versionMap, fromVi, toTime);
    }
}
