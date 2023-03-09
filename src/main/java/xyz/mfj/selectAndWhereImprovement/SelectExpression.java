package xyz.mfj.selectAndWhereImprovement;

import org.apache.commons.lang3.tuple.Pair;

import xyz.mfj.tableStructures.RowVector;

public abstract class SelectExpression implements Expression{
    protected Aggregator aggregator;
    
    public SelectExpression() {
        setAggregator();
    }
    
    public abstract void setAggregator();
    public abstract Pair<String, Class<?>> resultSchema();
    
    public boolean hasAggregator() {
        return aggregator != null;
    }
    
    /**
     * 计算并让聚合函数收集数据
     * @param row 一行数据
     * @param flag 有效时间起止时间标记
     */
    public void evalAndCollect(RowVector row, boolean flag) {
        aggregator.collect(evaluate(row), flag);
    }
    
    /**
     * 获取聚合结果
     * @return 聚合结果
     */
    public Object aggregate() {
        return aggregator.aggregate();
    }
    
}
