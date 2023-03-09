package xyz.mfj.query;

import xyz.mfj.tableStructures.Table;

public interface TemporalQueryExecutor {
    /**
     * 执行时态查询并返回结果表
     * @return 结果表
     */
    public Table execute();
}
