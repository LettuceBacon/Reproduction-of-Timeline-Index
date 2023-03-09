package xyz.mfj.query;

import xyz.mfj.selectAndWhereImprovement.SelectExpression;
import xyz.mfj.selectAndWhereImprovement.WhereExpression;

public interface TemporalQueryExecutorBuilder {
    /**
     * FROM语句对应的方法
     * @param tableName 时态表名
     * @return
     */
    public TemporalQueryExecutorBuilder fromTable(String tableName);
    
    /**
     * WHERE语句对应的方法
     * @param whereExpression where表达式
     * @return
     */
    public TemporalQueryExecutorBuilder withWhereClause(WhereExpression whereExpression);
    
    /**
     * SELECT语句对应的方法
     * @param selectExpression select表达式，包括聚合函数
     * @return
     */
    public TemporalQueryExecutorBuilder withSelectClause(SelectExpression selectExpression);

    public TemporalQueryExecutor build();
}
