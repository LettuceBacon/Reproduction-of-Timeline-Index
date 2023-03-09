package xyz.mfj.selectAndWhereImprovement;

public interface Aggregator {
    /**
     * 将一个数据收集到聚合函数的辅助结构中，对源表中的每一行应用一次
     * @param value 表在某行某列的一个数据
     * @param flag 有效时间起止实践标记，true为起始时间，false为终止时间
     */
    public void collect(Object value, boolean flag);
    
    /**
     * 聚合算法后处理并返回一个聚合结果数据
     * @return 一个聚合结果数据
     */
    public Object aggregate();
}
