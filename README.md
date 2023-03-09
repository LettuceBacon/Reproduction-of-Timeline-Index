# 复现timeline索引

## 论文名称
Timeline index: a unified data structure for processing queries on temporal data in SAP HANA

## TODO
- [x] 准备tpc-bih测试数据，包括sf=0.1和sf=1的数据
- [x] 实现时态表表结构
- [x] 实现VersionMap和EventList结构
- [x] 设计基于有效时间的索引构造方法
- [x] 实现表和索引构造方法
- [x] 实现时间旅行查询算法
- [x] 实现SUM、MAX、MIN聚合函数 
- [x] 实现时态分组查询算法
- [x] 优化存储结构，降低内存占用，让sf=1规模的数据可以运行在4GB内存中
- [] 使用或实现一个数据库类型系统
- [] 实现COUNT、AVG聚合函数
- [] 设计并实现checkpoint结构
- [] 设计并实现时态连接查询

## 细节设计
1. 时态表和timeline索引都以列存表为基础实现
2. 索引没有基于事务时间建立，而是基于有效时间
3. 由于基于有效时间，有效时间通常不会像事务id一样有序递增且由长整型存储，因此构造算法中没有采用计数排序，具体实现在xyz.mfj.tableStructures.TimelineIndex中
4. 复现的功能只能在限制的场景和条件下才能正常使用，具体限制在下一节详述
5. 每个新的查询都要通过建造者模式创建一个新的查询执行器，使用sql语义对应的方法配置查询，如`FROM`语句对应`TimeTravelQueryExecutorBuilder::fromTable`方法


## 运行限制
1. sql语义限制  
    时间旅行查询的sql语句  
    ```sql
    SELECT col [, col]
    FROM table
    [WHERE predicate [, predicate]]
    PERIOD table.period CONTAINS date
    ```
    时态分组查询的sql语句  
    ```sql
    SELECT col [, col]
    FROM table
    [WHERE predicate [, predicate]]
    GROUP BY PERIOD table.period [OVERLAPS (date, date)]
    ```
    时态连接查询的sql语句  
    ```sql
    SELECT col [, col]
    FROM table
    TEMPORAL JOIN table ON table.period OVERLAPS table.period
    [WHERE predicate [, predicate]]
    ```
    
    其中`col`是 `[agg(] expr [)]`  

    其中`expr`是 `literal_value(即字面值) | table.col(FROM表的列) | literal_value op expr | table.col op expr`  

    其中`op`是 `+ | - | * | / | = | >= | > | <= | < | !=`  

    其中`agg`是 `SUM | COUNT | AVG | MAX | MIN | ANY`  

    其中`table`是某个时态表  

    其中`table.period`是某个时态表的某一个有效时间  

    其中`predicate`是 `expr` 但是结果只能返回布尔值  

    其中date是 `literal_value` 但是只能是时间字面值  

2. 在应用时态聚合同时把有效时间查询出来，需要在起始时间上应用MAX聚合，在终止时间上应用MIN聚合，参照test中`xyz.mfj.query.TimeTravelQueryDebug::TimeTravelQueryWithSumAggDebug`

3. 数据类型仅限tpch基准线测试所使用的类型（类型系统还未实现）

## 内存测量需要的jvm执行参数
```json
"-javaagent:{HOME}/.m2/repository/net/sourceforge/sizeof/0.2.2/sizeof-0.2.2.jar",
"--add-opens",
"java.base/java.lang=ALL-UNNAMED",
"--add-opens",
"java.base/java.math=ALL-UNNAMED",
"--add-opens",
"java.base/java.util=ALL-UNNAMED",
"--add-opens",
"java.sql/java.sql=ALL-UNNAMED",
"--add-opens",
"java.base/java.lang.ref=ALL-UNNAMED",
"--add-opens",
"java.base/java.io=ALL-UNNAMED"
```