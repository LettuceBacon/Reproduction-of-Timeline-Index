package xyz.mfj.query;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.Library;
import xyz.mfj.selectAndWhereImprovement.SelectExpression;
import xyz.mfj.selectAndWhereImprovement.WhereExpression;
import xyz.mfj.tableStructures.BooleanColumnVector;
import xyz.mfj.tableStructures.ColumnStoreTable;
import xyz.mfj.tableStructures.IntegerColumnVector;
import xyz.mfj.tableStructures.RowVector;
import xyz.mfj.tableStructures.TableSchema;
import xyz.mfj.tableStructures.TimelineIndex;
import xyz.mfj.tableStructures.TimelineIndex.RowIndexAtVersion;

public class TemporalGroupingQueryExecutor implements TemporalQueryExecutor{
    private static Logger log = LoggerFactory.getLogger(TemporalGroupingQueryExecutor.class);
    
    private ColumnStoreTable temporalTable;
    private TimelineIndex timelineIndex;
    private Date startTime;
    private Date endTime;
    private WhereExpression whereExpression;
    private List<SelectExpression> selectExpressions;

    private TemporalGroupingQueryExecutor() {}
    
    private TemporalGroupingQueryExecutor(ColumnStoreTable temporalTable,
        TimelineIndex timelineIndex,
        Date startTime,
        Date endTime,
        WhereExpression whereExpression,
        List<SelectExpression> selectExpressions)
    {
        this.temporalTable = temporalTable;
        this.timelineIndex = timelineIndex;
        this.startTime = startTime;
        this.endTime = endTime;
        this.whereExpression = whereExpression;
        this.selectExpressions = selectExpressions;
    }
    
    public static class TemporalGroupingQueryExecutorBuilder implements TemporalQueryExecutorBuilder{
        private ColumnStoreTable temporalTable;
        private TimelineIndex timelineIndex;
        private Date startTime;
        private Date endTime;
        private WhereExpression whereExpression;
        private List<SelectExpression> selectExpressions;
        
        public TemporalGroupingQueryExecutorBuilder() {}
        
        public TemporalGroupingQueryExecutorBuilder fromTable(String tableName) {
            this.temporalTable = (ColumnStoreTable)Library
                .getInstance()
                .getTableByName(tableName);
            if (temporalTable == null) {
                log.error("There doesn't exist a table {}", tableName);
                System.exit(1);
            }
            if (!temporalTable.isTemporal()) {
                log.error("Table is non-temporal!");
                System.exit(1);
            }
            return this;
        }
    
        public TemporalGroupingQueryExecutorBuilder groupByPeriodOverlaps(String applicationPeriodName,Date startTime, Date endTime)
        {
            if (this.temporalTable == null) {
                log.error("GROUP BY should come after FROM TABLE!");
                System.exit(1);
            }
            if (!temporalTable.getApplicationPeriod().containsPeriod(applicationPeriodName)) 
            {
                log.error("Table doesn't contains {}!\n", applicationPeriodName);
                System.exit(1);
            }
            this.timelineIndex = Library.getInstance()
                .getTimelineIndexByName(
                    temporalTable.getTableName(), applicationPeriodName
                );
            if (this.timelineIndex == null) {
                log.error("There doesn't exist an index {}", applicationPeriodName);
                System.exit(1);
            }
            this.startTime = startTime;
            this.endTime = endTime;
            return this;
        }
        
        public TemporalGroupingQueryExecutorBuilder groupByPeriodOverlaps(String applicationPeriodName) {
            return groupByPeriodOverlaps(applicationPeriodName, new Date(0L), new Date(253402185600000L));
        }
    
        public TemporalGroupingQueryExecutorBuilder withWhereClause(WhereExpression whereExpression)
        {
            this.whereExpression = whereExpression;
            return this;
        }
    
        public TemporalGroupingQueryExecutorBuilder withSelectClause(SelectExpression selectExpression)
        {
            if (this.selectExpressions == null) {
                this.selectExpressions = new ArrayList<>();
            }
            this.selectExpressions.add(selectExpression);
            return this;
        }
        
        public TemporalGroupingQueryExecutor build() {
            if (selectExpressions == null 
                || temporalTable == null 
                || timelineIndex == null
                || startTime == null
                || endTime == null) 
            {
                log.error("Temporal grouping query lacks key clause(SELECT, FROM, GROUP BY)!");
                System.exit(1);
            }
            for (SelectExpression selectExpression : selectExpressions) {
                if (!selectExpression.hasAggregator()) {
                    log.error("In a temporal grouping query, all select clauses should have temporalAggregator or any()");
                    System.exit(1);
                }
            }
            return new TemporalGroupingQueryExecutor(temporalTable, 
                timelineIndex, 
                startTime, 
                endTime, 
                whereExpression, 
                selectExpressions);
        }
        
    }

    
    
    public ColumnStoreTable execute() {
        long stime = System.currentTimeMillis();
        
        BitSet validRows = new BitSet(temporalTable.getRowSize());
        int vi = 0;
        log.info("Checkpoint is not supported at this time, start linear scan from the beginning of index");
        
        
        IntegerColumnVector rowId = (IntegerColumnVector)timelineIndex.getEventList().getColumnAt(0);
        BooleanColumnVector flag = (BooleanColumnVector)timelineIndex.getEventList().getColumnAt(1);
        
        if (timelineIndex.getMinVersion().compareTo(endTime) > 0) {
            return null;
        }
        
        // 读到startTime之前的一个时间版本
        Iterator<RowIndexAtVersion> preIter = timelineIndex.iterator(vi, startTime);
        while (preIter.hasNext()) {
            RowIndexAtVersion rowIndexAtVersion = preIter.next();
            vi = rowIndexAtVersion.getVi();
            for (int ei = rowIndexAtVersion.getEiStart(); ei < rowIndexAtVersion.getEiEnd(); ei++) {
                validRows.set(rowId.getValue(ei), flag.getValue(ei));
            }
        }
        preIter = null;
        
        // 处理startTime之前的有效行，where筛选，sum计算所有有效行的和，max构造所有有效行的辅助结构
        int[] validRowIds = validRows.stream().toArray();
        for (int i = 0; i < validRowIds.length; i++) {
            RowVector row = temporalTable.getRowAt(validRowIds[i]);
            
            if (whereExpression != null) {
                if (!whereExpression.admit(row)) {
                    validRows.set(validRowIds[i], false);
                    row = null;
                }
            }
            if (row == null) continue;

            for (int j = 0; j < selectExpressions.size(); j++) {
                selectExpressions.get(j).evalAndCollect(row, true);
            }
        }
        
        int resultColumnNumber = selectExpressions.size();
        List<Pair<String, Class<?>>> columnList = new ArrayList<>(resultColumnNumber);
        for (SelectExpression selectExpression : selectExpressions) {
            columnList.add(selectExpression.resultSchema());
        }
        ColumnStoreTable resultTable = new ColumnStoreTable(
            new TableSchema(columnList), 
            null,
            "resultTable"
        );
        
        // 从startTime读到endTime之前的一个时间版本
        Iterator<RowIndexAtVersion> iter = timelineIndex.iterator(vi + 1, endTime);
        while (iter.hasNext()) {
            RowIndexAtVersion rowIndexAtVersion = iter.next();
            vi = rowIndexAtVersion.getVi();
            for (int ei = rowIndexAtVersion.getEiStart(); ei < rowIndexAtVersion.getEiEnd(); ei++) {
                RowVector row = temporalTable.getRowAt(rowId.getValue(ei));
                
                if (whereExpression != null) {
                    if (!whereExpression.admit(row)) {
                        validRows.set(rowId.getValue(ei), false);
                        row = null;
                    }
                    else {
                        validRows.set(rowId.getValue(ei), flag.getValue(ei));
                    }
                }
                if (row == null) continue;
                
                for (int j = 0; j < selectExpressions.size(); j++) {
                    selectExpressions.get(j).evalAndCollect(row, flag.getValue(ei));
                }
            }

            // 生成一个时间版本上的聚合结果
            RowVector resultRow = new RowVector(resultColumnNumber);
            for (int j = 0; j < resultColumnNumber; j++) {
                resultRow.set(j, selectExpressions.get(j).aggregate());
            }
            resultTable.addRowAt(resultTable.getRowSize(), resultRow);
            
        }
        iter = null;
        
        
        long etime = System.currentTimeMillis();
        System.out.printf("Query time taken: %d ms\n", etime - stime);
        
        return resultTable;
    }
}
