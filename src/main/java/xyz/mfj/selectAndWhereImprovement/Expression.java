package xyz.mfj.selectAndWhereImprovement;

import xyz.mfj.tableStructures.RowVector;

public interface Expression {
    /**
     * 将一行数据计算为一个结果数据
     * @param row 一行数据
     * @return 一个结果数据
     */
    public Object evaluate(RowVector row);
}
