package xyz.mfj.selectAndWhereImprovement;

import xyz.mfj.tableStructures.RowVector;

public abstract class WhereExpression implements Expression{
    public boolean admit(RowVector row) {
        return (boolean)evaluate(row);
    }
}
