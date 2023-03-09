package xyz.mfj.tableStructures;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import net.sourceforge.sizeof.SizeOf;

public class TableIndexDebug {
    String lineitembihFileName;
    String tableName;
    List<Pair<String, Class<?>>> columnList;
    Triple<String, String, String> activeTime;

    @BeforeEach
    public void declareTable() {
        lineitembihFileName = 
            "/home/mfj/lineitembih/lineitembih-part-1-sf-0dot1.dat";
        // lineitembihFileName = 
        //     "/home/mfj/lineitembih/lineitembih-part-1-sf-1.dat";

        tableName = "lineitembih";
        columnList = new ArrayList<>();
        columnList.add(Pair.of("orderKey", long.class));
        columnList.add(Pair.of("partKey", long.class));
        columnList.add(Pair.of("supplierKey", long.class));
        columnList.add(Pair.of("lineNumber", int.class));
        columnList.add(Pair.of("quantity", BigDecimal.class));
        columnList.add(Pair.of("extendedPrice", BigDecimal.class));
        columnList.add(Pair.of("discount", BigDecimal.class));
        columnList.add(Pair.of("tax", BigDecimal.class));
        columnList.add(Pair.of("returnFlag", String.class));
        columnList.add(Pair.of("status", String.class));
        columnList.add(Pair.of("shipDate", Date.class));
        columnList.add(Pair.of("commitDate", Date.class));
        columnList.add(Pair.of("receiptDate", Date.class));
        columnList.add(Pair.of("shipInstructions", String.class));
        columnList.add(Pair.of("shipMode", String.class));
        columnList.add(Pair.of("comment", String.class));
        columnList.add(Pair.of("activeTimeBegin", Date.class));
        columnList.add(Pair.of("activeTimeEnd", Date.class));
        activeTime = Triple.of("activeTime", "activeTimeBegin", "activeTimeEnd");
    }

    @Test
    public void tableAndIndexConstructorTest() {
        
        ColumnStoreTable lineitembih = new ColumnStoreTable(
            lineitembihFileName, 
            new TableSchema(columnList), 
            tableName,
            List.of(activeTime)
        );
        
        SizeOf.skipStaticField(true);
        System.out.println(
            String.format("table size %d byte(s)", SizeOf.deepSizeOf(lineitembih)
        ));
        
        TimelineIndex lineitembihIndex = new TimelineIndex(
            lineitembih, 
            lineitembih.getApplicationPeriod().getApplicationPeriodNames().get(0)
        );

        System.out.println();
        System.out.println(
            String.format("index size %d byte(s)", SizeOf.deepSizeOf(lineitembihIndex)
        ));
    }
}
