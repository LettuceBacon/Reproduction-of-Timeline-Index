package xyz.mfj.operators;

import java.util.Comparator;

public interface CompareOperator {
    public int compare(Object a, Object b);
    public Comparator<Object> getOper();
}
