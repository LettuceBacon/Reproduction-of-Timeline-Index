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

public class TimeTravelQueryExecutor implements TemporalQueryExecutor {
    private static Logger log = LoggerFactory.getLogger(TimeTravelQueryExecutor.class);
    
    private ColumnStoreTable temporalTable;
    private TimelineIndex timelineIndex;
    private Date containsTime;
    private WhereExpression whereExpression;
    private List<SelectExpression> selectExpressions;
    
    private TimeTravelQueryExecutor() {}
    
    private TimeTravelQueryExecutor(ColumnStoreTable temporalTable,
        TimelineIndex timelineIndex,
        Date containsTime,
        WhereExpression whereExpression,
        List<SelectExpression> selectExpressions)
    {
        this.temporalTable = temporalTable;
        this.timelineIndex = timelineIndex;
        this.containsTime = containsTime;
        this.whereExpression = whereExpression;
        this.selectExpressions = selectExpressions;
    }
    
    public static class TimeTravelQueryExecutorBuilder implements TemporalQueryExecutorBuilder {
        private ColumnStoreTable temporalTable;
        private TimelineIndex timelineIndex;
        private Date containsTime;
        private WhereExpression whereExpression;
        private List<SelectExpression> selectExpressions;
        
        public TimeTravelQueryExecutorBuilder() {}
        
        public TimeTravelQueryExecutorBuilder fromTable(String tableName) {
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
        
        public TimeTravelQueryExecutorBuilder periodContainsTime(String applicationPeriodName, Date containsTime)
        {
            if (this.temporalTable == null) {
                log.error("PERIOD CONTAINS should come after FROM TABLE!");
                System.exit(1);
            }
            if (!temporalTable.getApplicationPeriod().containsPeriod(applicationPeriodName)) 
            {
                log.error("Table doesn't contains %s!\n", applicationPeriodName);
                System.exit(1);
            }
            this.timelineIndex = Library.getInstance()
                .getTimelineIndexByName(
                    temporalTable.getTableName(), applicationPeriodName
                );
            if (this.timelineIndex == null) {
                log.error("There doesn't exist an index %s", applicationPeriodName);
                System.exit(1);
            }
            this.containsTime = containsTime;
            return this;
        }
        
        public TimeTravelQueryExecutorBuilder withWhereClause(WhereExpression whereExpression)
        {
            this.whereExpression = whereExpression;
            return this;
        }
        
        public TimeTravelQueryExecutorBuilder withSelectClause(SelectExpression selectExpression)  {
            if (this.selectExpressions == null) {
                this.selectExpressions = new ArrayList<>();
            }
            this.selectExpressions.add(selectExpression);
            return this;
        }
        
        public TimeTravelQueryExecutor build() {
            if (selectExpressions == null 
                || temporalTable == null 
                || timelineIndex == null
                || containsTime == null) 
            {
                log.error("Time travel query lacks key clause(SELECT, FROM, PERIOD CONTAINS)!");
                System.exit(1);
            }
            boolean hasAggregator = selectExpressions.get(0).hasAggregator();
            for (SelectExpression selectExpression : selectExpressions) {
                if (selectExpression.hasAggregator() != hasAggregator) {
                    log.error("If one temporalAggregator is given, all other select clauses should have temporalAggregator or any()");
                    System.exit(1);
                }
            }
            return new TimeTravelQueryExecutor(temporalTable, 
                timelineIndex, 
                containsTime, 
                whereExpression,
                selectExpressions);
        }
    }

    public ColumnStoreTable execute() {
        long stime = System.currentTimeMillis();

        BitSet validRows = new BitSet(temporalTable.getRowSize());
        
        int vi = 0; // versionMap的行id
        log.info("Checkpoint is not supported at this time, start linear scan from the beginning of index");
        // 先顺序读checkpoints，找到起始时间
        // 没有checkpoints，起始时间为versionMap第一个时间

        IntegerColumnVector rowId = (IntegerColumnVector)timelineIndex.getEventList().getColumnAt(0);
        BooleanColumnVector flag = (BooleanColumnVector)timelineIndex.getEventList().getColumnAt(1);


        if (timelineIndex.getMinVersion().compareTo(containsTime) > 0) {
            return null;
        }
        
        // 遍历timeline，生成containsTime上的有效行
        Iterator<RowIndexAtVersion> iter = timelineIndex.iterator(vi, new Date(containsTime.getTime() + 1));
        while (iter.hasNext()) {
            RowIndexAtVersion rowIndexAtVersion = iter.next();
            vi = rowIndexAtVersion.getVi();
            for (int ei = rowIndexAtVersion.getEiStart(); ei < rowIndexAtVersion.getEiEnd(); ei++) {
                validRows.set(rowId.getValue(ei), flag.getValue(ei));
            }
        }
        iter = null;
        
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
        
        // 遍历每一个有效行，应用where语句筛选，满足条件的应用select语句映射到结果表中
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

            if (selectExpressions.get(0).hasAggregator()) { // 如果有聚合函数，计算并收集数据
                for (int j = 0; j < selectExpressions.size(); j++) {
                    selectExpressions.get(j).evalAndCollect(row, true);
                }
            }
            else { // 没有聚合函数，计算后直接生成一行结果
                RowVector resultRow = new RowVector(resultColumnNumber);
                for (int j = 0; j < resultColumnNumber; j++) {
                    resultRow.set(j, selectExpressions.get(j).evaluate(row));
                }
                resultTable.addRowAt(resultTable.getRowSize(), resultRow);
            }
        }
        if (selectExpressions.get(0).hasAggregator()) {
            RowVector resultRow = new RowVector(resultColumnNumber);
            for (int j = 0; j < resultColumnNumber; j++) {
                resultRow.set(j, selectExpressions.get(j).aggregate());
            }
            resultTable.addRowAt(resultTable.getRowSize(), resultRow);
        }

        long etime = System.currentTimeMillis();
        System.out.printf("Query time taken: %d ms\n", etime - stime);

        return resultTable;
    }

    
}
