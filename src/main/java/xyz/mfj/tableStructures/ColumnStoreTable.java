package xyz.mfj.tableStructures;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.List;
import java.util.Vector;

import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;


import xyz.mfj.utils.TypeUtil;

public class ColumnStoreTable implements Table{
    private static Logger log = LoggerFactory.getLogger(ColumnStoreTable.class);
    
    /**
     * 列的扩容基数，每次扩容若干个基数大小
     */
    private static final int DEFAULTINCREMENTBASE = 512;
    
    /**
     * 根据期望的大小获取列扩容实际应增大到多少
     * @param expectedSize 期望的列大小
     * @return 列扩容后的实际大小
     */
    private static final int newRowNumber(int expectedSize) {
        return (expectedSize / DEFAULTINCREMENTBASE + 1) * DEFAULTINCREMENTBASE;
    }
    
    private Vector<ColumnVector> columns;
    /**
     * 当前表中存储了多少行
     */
    private int rowSize;
    /**
     * 表当前容量（行数上限）
     */
    private int rowNumber;
    private TableSchema schema;
    private String tableName;
    private ApplicationPeriod applicationPeriod;
    

    public ColumnStoreTable() {}

    /**
     * 从文本文件读时态数据，按照模式解析，然后存储在内存中
     * @param textFileName 文本文件绝对路径
     * @param schema 表模式
     * @param tableName 表名
     * @param applicationPeriodList 有效时间列表，每个元素是（有效时间名称、有效时间起始时间名称、有效时间终止时间名称）三元组
     */
    public ColumnStoreTable(String textFileName, 
        TableSchema schema, 
        String tableName,
        List<Triple<String, String, String>> applicationPeriodList) 
    {
        long stime = System.currentTimeMillis();
        
        int columnNumber = schema.getColumnSize();
        List<Class<?>> columnTypes = schema.getColumnTypes();
        this.tableName = tableName;
        this.schema = schema;
        this.rowNumber = 0;
        this.rowSize = 0;
        this.columns = new Vector<>(columnNumber);
        this.applicationPeriod = applicationPeriodList != null ? 
            new ApplicationPeriod(applicationPeriodList) : null;

        File textFile = new File(textFileName);
        try (
            LineNumberReader lnr = new LineNumberReader(new FileReader(textFile));
        ) {
            lnr.skip(Integer.MAX_VALUE);
            this.rowNumber = newRowNumber(lnr.getLineNumber());
        } catch(IOException e) {
            log.error("{}", e);
        }

        for (int i = 0; i < columnNumber; i++) {
            columns.add(ColumnVector.newColumnVector(columnTypes.get(i)));
        }
        
        for (ColumnVector column : columns) {
            column.resize(this.rowNumber);
        }

        try (FileReader fr = new FileReader(textFile)) {
            CSVReader reader = new CSVReaderBuilder(fr)
                .withCSVParser(
                    new CSVParserBuilder()
                        .withSeparator('|') // tpc-bih和tpch数据文件的列分隔符
                        .build()
                )
                .build();

            String[] line = null;
            while ((line = reader.readNext()) != null) {
                if (line.length < columnNumber) {
                    throw new ArrayIndexOutOfBoundsException("Column number in table text is less than required number of table schema!");
                }
                for (int i = 0; i < columnNumber; i++) {
                    columns.get(i).setValue(
                        this.rowSize, 
                        TypeUtil.castStringTo(columnTypes.get(i), line[i])
                    );
                }
                this.rowSize++;
            }
            log.info("read {} lines\n", this.rowSize);
        } catch (CsvValidationException | IOException e) {
            log.info("read {} lines\n", this.rowSize);
            log.error("{}", e);
            System.exit(1);
        }

        
        long etime = System.currentTimeMillis();
        log.info("table construction time {} ms\n", etime - stime);
    }

    /**
     * 创建一个空表
     * @param schema 表模式
     * @param applicationPeriodList 有效时间列表，每个元素是（有效时间名称、有效时间起始时间名称、有效时间终止时间名称）三元组
     * @param tableName 表名
     */
    public ColumnStoreTable(TableSchema schema, 
        List<Triple<String, String, String>> applicationPeriodList,
        String tableName) 
    {
        int columnNumber = schema.getColumnSize();
        List<Class<?>> columnTypes = schema.getColumnTypes();
        this.tableName = tableName;
        this.rowNumber = 0;
        this.rowSize = 0;
        this.schema = schema;
        this.columns = new Vector<>(columnNumber, 1);
        this.applicationPeriod = applicationPeriodList != null ? 
            new ApplicationPeriod(applicationPeriodList) : null;

        for (int i = 0; i < columnNumber; i++) {
            columns.add(ColumnVector.newColumnVector(columnTypes.get(i)));
        }
    }

    @Override
    public TableSchema getTableSchema() {
        return this.schema;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }
    
    @Override
    public int getRowSize() {
        return this.rowSize;
    }

    @Override
    public int getColumnSize() {
        return schema.getColumnSize();
    }
    
    @Override
    public boolean isColumnar() {
        return true;
    }
    
    @Override
    public boolean isTemporal() {
        return applicationPeriod != null;
    }

    public Object getValueAt(int columnIndex, int rowIndex) {
        return columns.get(columnIndex).getValue(rowIndex);
    }

    /**
     * 获取表的某一列数据
     * @param columnIndex 列号
     * @return 某一列数据
     */
    public ColumnVector getColumnAt(int columnIndex) {
        return columns.get(columnIndex);
    }
    
    /**
     * 获取表的某一行数据
     * @param rowIndex 行号
     * @return 某一行数据
     */
    public RowVector getRowAt(int rowIndex) {
        int columnNumber = schema.getColumnSize();
        RowVector row = new RowVector(columnNumber);
        for (int i = 0; i < columnNumber; i++) {
            row.set(i, columns.get(i).getValue(rowIndex));
        }
        return row;
    }
    
    /**
     * 在指定行号插入一行。如果当前容量不足，进行列的扩容，然后插入。
     * @param rowIndex 表中行号
     * @param row 待插入的行
     */
    public void addRowAt(int rowIndex, RowVector row) {
        if (rowIndex >= rowNumber) {
            rowNumber = newRowNumber(rowIndex + 1);
            for (ColumnVector column : columns) {
                column.resize(rowNumber);
            }
        }
        int columnSize = schema.getColumnSize();
        for (int i = 0; i < columnSize; i++) {
            columns.get(i).setValue(rowIndex, row.get(i));
        }
        rowSize++;
    }

    public ApplicationPeriod getApplicationPeriod() {
        return applicationPeriod;
    }

    @Override
    public String toString() {
        int columnNumber = schema.getColumnSize();
        StringBuilder sb = new StringBuilder();
        for (String columnName : schema.getColumnNames()) {
            sb.append(String.format("%s|", columnName));
        }
        sb.append('\n');
        for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            for (int columnIndex = 0; columnIndex < columnNumber; columnIndex++) {
                sb.append(String.format("%s|", columns.get(columnIndex).getValue(rowIndex).toString()));
            }
            sb.append('\n');
        }

        return sb.toString();
    }

}
