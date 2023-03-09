package xyz.mfj.query;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import xyz.mfj.Library;
import xyz.mfj.tableStructures.ColumnStoreTable;
import xyz.mfj.tableStructures.RowVector;
import xyz.mfj.tableStructures.TableSchema;
import xyz.mfj.tableStructures.TimelineIndex;
import xyz.mfj.query.TemporalGroupingQueryExecutor.TemporalGroupingQueryExecutorBuilder;
import xyz.mfj.selectAndWhereImprovement.SelectExpression;
import xyz.mfj.selectAndWhereImprovement.TemporalMaxAggregator;
import xyz.mfj.selectAndWhereImprovement.TemporalMinAggregator;
import xyz.mfj.selectAndWhereImprovement.TemporalSumAggregator;
import xyz.mfj.selectAndWhereImprovement.WhereExpression;


public class TemporalAggregateQueryDebug {
    private Library lib;
    
    @BeforeEach
    public void prepareTableAndIndex() {
        lib = Library.getInstance();
        // String lineitembihFileName = "/home/mfj/lineitembih/lineitembih-part-1-sf-0dot1.dat";
        String lineitembihFileName = "/home/mfj/lineitembih/lineitembih-part-1-sf-1.dat";

        String tableName = "lineitembih";
        List<Pair<String, Class<?>>> columnList = new ArrayList<>();
        columnList.add(Pair.of("orderKey", long.class));
        columnList.add(Pair.of("partKey", long.class));
        columnList.add(Pair.of("supplierKey", long.class));
        columnList.add(Pair.of("lineNumber", int.class));
        columnList.add(Pair.of("quantity", BigDecimal.class));
        columnList.add(Pair.of("extendedPrice", BigDecimal.class));
        columnList.add(Pair.of("discount", BigDecimal.class));
        columnList.add(Pair.of("tax", BigDecimal.class));
        columnList.add(Pair.of("returnFlag", String.class));
        columnList.add(Pair.of("status", String.class));
        columnList.add(Pair.of("shipDate", Date.class));
        columnList.add(Pair.of("commitDate", Date.class));
        columnList.add(Pair.of("receiptDate", Date.class));
        columnList.add(Pair.of("shipInstructions", String.class));
        columnList.add(Pair.of("shipMode", String.class));
        columnList.add(Pair.of("comment", String.class));
        columnList.add(Pair.of("activeTimeBegin", Date.class));
        columnList.add(Pair.of("activeTimeEnd", Date.class));
        Triple<String, String, String> activeTime = Triple.of("activeTime", "activeTimeBegin", "activeTimeEnd");

        ColumnStoreTable lineitembih = new ColumnStoreTable(
            lineitembihFileName, 
            new TableSchema(columnList), 
            tableName,
            List.of(activeTime)
        );
        TimelineIndex lineitembihIndex = new TimelineIndex(
            lineitembih, 
            lineitembih.getApplicationPeriod().getApplicationPeriodNames().get(0)
        );

        lib.cacheTable(lineitembih);
        lib.cacheTimelineIndex(lineitembihIndex);
    }

    @Test
    public void cumulativeAggregateQueryDebug() {
        TemporalGroupingQueryExecutor executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("lineitembih")
            // .groupByPeriodOverlaps("activeTime")
            .groupByPeriodOverlaps("activeTime", Date.valueOf("1996-02-12"), Date.valueOf("1996-03-12"))
            .withSelectClause(new SelectExpression() {

                @Override
                public Object evaluate(RowVector row) {
                    BigDecimal quantity = (BigDecimal)row.get(4);
                    return quantity;
                }

                @Override
                public void setAggregator() {
                    this.aggregator = new TemporalSumAggregator(BigDecimal.class);
                }

                @Override
                public Pair<String, Class<?>> resultSchema() {
                    return Pair.of("sum(quantity)", BigDecimal.class);
                }
                
            })
            .withWhereClause(new WhereExpression() {

                @Override
                public Object evaluate(RowVector row) {
                    String returnFlag = (String)row.get(8);
                    return returnFlag.equals("N");
                }
                
            })
            .build();
            
        ColumnStoreTable resultTable = executor.execute();
        System.out.println(resultTable);
        System.out.printf("Fetched: %d row(s)\n", resultTable.getRowSize());
    }
    
    @Test
    public void selectiveAggregateQueryDebug() {
        TemporalGroupingQueryExecutor executor = new TemporalGroupingQueryExecutorBuilder()
            .fromTable("lineitembih")
            // .groupByPeriodOverlaps("activeTime")
            .groupByPeriodOverlaps("activeTime", Date.valueOf("1996-02-12"), Date.valueOf("1996-03-12"))
            .withSelectClause(new SelectExpression() {

                @Override
                public Object evaluate(RowVector row) {
                    BigDecimal quantity = (BigDecimal)row.get(6);
                    return quantity;
                }

                @Override
                public void setAggregator() {
                    this.aggregator = new TemporalMaxAggregator(BigDecimal.class);
                }

                @Override
                public Pair<String, Class<?>> resultSchema() {
                    return Pair.of("max(discount)", BigDecimal.class);
                }
                
            })
            .withSelectClause(new SelectExpression() {

                @Override
                public Object evaluate(RowVector row) {
                    Date activeTimeBegin = (Date)row.get(16);
                    return activeTimeBegin;
                }

                @Override
                public void setAggregator() {
                    this.aggregator = new TemporalMaxAggregator(Date.class);
                }

                @Override
                public Pair<String, Class<?>> resultSchema() {
                    return Pair.of("activeTimeBegin", Date.class);
                }
                
            })
            .withSelectClause(new SelectExpression() {

                @Override
                public Object evaluate(RowVector row) {
                    Date activeTimeEnd = (Date)row.get(17);
                    return activeTimeEnd;
                }

                @Override
                public void setAggregator() {
                    this.aggregator = new TemporalMinAggregator(Date.class);
                }

                @Override
                public Pair<String, Class<?>> resultSchema() {
                    return Pair.of("activeTimeEnd", Date.class);
                }
                
            })
            .withWhereClause(new WhereExpression() {

                @Override
                public Object evaluate(RowVector row) {
                    int lineNumber = (int)row.get(3);
                    return lineNumber == 2;
                }
                
            })
            .build();
            
        ColumnStoreTable resultTable = executor.execute();
        System.out.println(resultTable);
        System.out.printf("Fetched: %d row(s)\n", resultTable.getRowSize());
    }

}
