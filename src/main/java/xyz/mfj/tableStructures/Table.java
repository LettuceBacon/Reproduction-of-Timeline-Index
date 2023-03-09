package xyz.mfj.tableStructures;

public interface Table {

    /**
     * 表是否是列式存储的
     * @return 列存表返回true，行存表返回false
     */
    public boolean isColumnar();
    
    /**
     * 表是否是时态表
     * @return 时态表返回true，非时态表返回false
     */
    public boolean isTemporal();
    
    /**
     * 获取表模式
     * @return 表模式
     */
    public TableSchema getTableSchema();
    
    /**
     * 获取表名
     * @return 表名
     */
    public String getTableName();
    
    /**
     * 获取表实际存储多少行
     * @return 表当前行数
     */
    public int getRowSize();
    
    /**
     * 获取表当前有多少列
     * @return 表当前列数
     */
    public int getColumnSize();
}
