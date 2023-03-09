package xyz.mfj;


import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
// import org.h2.value.ValueInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.util.TreeMap;
import java.util.Vector;


import net.sourceforge.sizeof.SizeOf;
import xyz.mfj.tableStructures.ColumnStoreTable;
import xyz.mfj.tableStructures.RowVector;
import xyz.mfj.tableStructures.TableSchema;
import xyz.mfj.tableStructures.TimelineIndex;
import xyz.mfj.tableStructures.TimelineIndex.RowIndexAtVersion;
import xyz.mfj.utils.TypeUtil;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    private static Logger log = LoggerFactory.getLogger(AppTest.class);
    
    @Test
    public void superClassDebug() {
        Number abc = Integer.valueOf(2);
        Long wasd = Long.valueOf(123);
        log.info(String.valueOf(Number.class.isInstance(abc)));
        log.info(String.valueOf(Number.class.isInstance(wasd)));
        log.info(String.valueOf(Number.class.isInstance(new Object())));
        System.out.println(Number.class.isAssignableFrom(Number.class));
        System.out.println(Number.class.isAssignableFrom(Long.class));
        System.out.println(Number.class.isAssignableFrom(Object.class));
    }
    
    
    @Test
    public void newRowVector() {
        RowVector abc = new RowVector(Integer.valueOf(1), String.valueOf(2));
        System.out.println(abc.get(0));
        System.out.println(abc.get(1));
        // System.out.println(abc.get(2));
    }
    
    @Test
    public void opencsvDebug() {
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(new File("/home/mfj/test.csv"))).build()) {
            String [] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                // nextLine[] is an array of values from the line
                System.out.println(nextLine[0] + nextLine[1] + nextLine[2] + nextLine[3]);
            }
        } catch (CsvValidationException | IOException e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void arrayGrowAndShrink() {
        int[] abs = new int[100];
        abs[99] = 2;
        abs = Arrays.copyOf(abs, 200);
        System.out.println(abs[99]);
        System.out.println(abs[199]);
        abs = Arrays.copyOf(abs, 50);
        // System.out.println(abs[99]);
        
        String jjc = "jjc";
        String jjjjjj = "jjjjjj";
        byte[][] values = new byte[2][];
        values[0] = jjc.getBytes();
        values[1] = jjjjjj.getBytes();
        System.out.println(values[0].length);
        System.out.println(values[1].length);
    }
    
    @Test
    public void vectorAndArrayListMemUsageBenchmark() {
        int size = 1000000;
        
        Vector<Integer> abc = new Vector<>(10000, 1000);
        ArrayList<Integer> wasd = new ArrayList<>(10000);
        int[] jjc = new int[size];
        long stime = System.currentTimeMillis();
        long etime = 0;
        for (int i = 0; i < size; i++) {
            abc.add(i);
        }
        etime = System.currentTimeMillis();
        System.out.printf("vector construction cost %d ms\n", etime - stime);
        
        stime = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            wasd.add(i);
        }
        etime = System.currentTimeMillis();
        System.out.printf("arraylist construction cost %d ms\n", etime - stime);
        
        stime = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            jjc[i] = i;
        }
        etime = System.currentTimeMillis();
        System.out.printf("array construction cost %d ms\n", etime - stime);
        
        SizeOf.skipStaticField(true);
        System.out.println(SizeOf.humanReadable(SizeOf.deepSizeOf(abc)));
        System.out.println(SizeOf.humanReadable(SizeOf.deepSizeOf(wasd)));
        System.out.println(SizeOf.humanReadable(SizeOf.deepSizeOf(jjc)));
        
        // Vector<ValueInteger> www = new Vector<ValueInteger>(10000);
        // for (int index = 0; index < 10000; index++) {
        //     www.add(ValueInteger.get(index));
        // }
        
        // SizeOf.skipStaticField(true);
        // System.out.println(SizeOf.deepSizeOf(www));
    }
    
    @Test
    public void emptyObjectSize() {
        TreeMap<String, Integer> a = new TreeMap<>();
        TreeMap<Integer, Integer> b = new TreeMap<>();
        TreeMap<String, String> c = new TreeMap<>();
        SizeOf.skipStaticField(true);
        System.out.println(SizeOf.deepSizeOf(a));
        System.out.println(SizeOf.deepSizeOf(b));
        System.out.println(SizeOf.deepSizeOf(c));
    }
    
    @Test
    public void genericTypePolymorphism() {
        
        NumericCounterTreeMap abc = new IntegerCounterTreeMap();
        abc.put(1, 15);
        System.out.println(abc.firstKey());
    }
    private interface NumericCounterTreeMap {
        public Number firstKey();
        public Number lastKey();
        public Integer get(Object key);
        public Integer put(Number key, Integer value);
    }
    
    private class IntegerCounterTreeMap implements NumericCounterTreeMap{
        private TreeMap<Integer, Integer> treeMap;
        
        public IntegerCounterTreeMap() {
            treeMap = new TreeMap<>();
        }
        
        @Override
        public Integer firstKey() {
            return treeMap.firstKey();
        }
        
        @Override
        public Integer lastKey() {
            return treeMap.lastKey();
        }
        
        @Override
        public Integer get(Object key) {
            return treeMap.get(key);
        }
        
        @Override
        public Integer put(Number key, Integer value) {
            return treeMap.put((Integer)key, value);
        }
        
        public boolean containsKey(Object key) {
            return treeMap.containsKey(key);
        }
        
        public Integer remove(Object key) {
            return treeMap.remove(key);
        }
    }
    
    // 一个Object对象，运行时通过反射赋值，且能够执行一个接口中声明的函数
    @Test
    public void RTTI() {
        Object abc = new SomeType(1);
        TreeMap<Integer, Integer> wasd = (TreeMap<Integer, Integer>)abc;
    }
    class SomeType {
        int i;
        
        public SomeType(int i) {
            this.i =  i;
        }
        
        @Override
        public String toString() {
            return String.valueOf(i);
        }
    }
    
    
    @Test
    public void genericTypeFunction() {
        ArrayList<? super Number> abc = new ArrayList<>();
        abc.add(1); // OK calling ArrayList::add(Object arg0)
        // System.out.println(abc);

        // jjc.add(new Object()); // ERROR
        // https://juejin.cn/post/7145468888536907790 泛型协变逆变

        // 泛型，编译时确定类型
        // 反射，运行时确定类型
        
        // 一个在编译时知道是Object的泛型类
        ArrayList<?> jjc = new ArrayList<>();
        
        ((ArrayList<Integer>)jjc).add(1);
        ((ArrayList<String>)jjc).add("abc");
        System.out.println(jjc.get(0));
        System.out.println(jjc.get(1));
        jjc.remove(0);
        
        MyArrayList list = new MyArrayList(Integer.class);
        list.add(1);
        Integer xxm = list.get(0);
        Long kjkj = list.get(0);
        
    }
    class MyArrayList {
        private static int INITLENGTH = 100;

        private Object[] values;
        private int size;
        private Class<?> type;
        
        public MyArrayList(Class<?> type) {
            this.values = new Object[INITLENGTH];
            this.size = 0;
            this.type = type;
        }
        
        public void add(Object value) {
            // 检查类型是否一致
            values[size++] = value; // 动态扩容
        }
        
        public <T> T get(int i) {
            return (T) values[i];
        }
        
    }
    
    class MyGenericMethod<T> {
        public T get(List<T> abc, int i) {
            return abc.get(i);
        }
        
        public void set(List<T> abc, int i, T value) {
            abc.set(i, value);
        }
    }
    
    
    @Test
    public void comparatorAndCompareFun() {
        System.out.println(Integer.valueOf(4).compareTo(Integer.valueOf(3))); // 1
        System.out.println(Integer.valueOf(4).compareTo(Integer.valueOf(4))); // 0
        System.out.println(Integer.valueOf(4).compareTo(Integer.valueOf(5))); // -1
        System.out.println(BigDecimal.valueOf(4).compareTo(BigDecimal.valueOf(3))); // 1
        System.out.println(BigDecimal.valueOf(4).compareTo(BigDecimal.valueOf(4))); // 0
        System.out.println(BigDecimal.valueOf(4).compareTo(BigDecimal.valueOf(5))); // -1
        System.out.println(Date.valueOf("1993-11-11").compareTo(Date.valueOf("1992-11-11"))); // 1
        System.out.println(Date.valueOf("1993-11-11").compareTo(Date.valueOf("1993-11-11"))); // 0
        System.out.println(Date.valueOf("1993-11-11").compareTo(Date.valueOf("1994-11-11"))); // -1
        System.out.println(Date.valueOf("1993-11-11").compareTo(new java.util.Date(0L)));
    }
    
    @Test
    public void minAndMaxDecimal() {
        System.out.println(BigDecimal.valueOf(Double.MIN_VALUE));
        System.out.println(BigDecimal.valueOf(Double.MAX_VALUE));
        System.out.println(Double.MIN_VALUE);
        System.out.println(Double.MAX_VALUE);
        
    }
    
    @Test
    public void maxMinDateTimestamp() {
        // Date abc = new Date(0);
        Date wasd = new Date(253402271999999L);
        // Date jjj = new Date(Long.MIN_VALUE);
        // Date iiaf = Date.valueOf("9999-12-31");
        // System.out.println(abc + " " + abc.getTime());
        System.out.println(wasd + " " + wasd.getTime());
        // System.out.println(jjj + " " + jjj.getTime());
        // System.out.println(iiaf + " " + iiaf.getTime());
        
        Timestamp t0 = new Timestamp(0L);
        Timestamp t1 = new Timestamp(253402185600000L);
        // 253402271999999
        // 253402185600000L
        Timestamp t2 = new Timestamp(Long.MIN_VALUE);
        Timestamp t3 = Timestamp.valueOf("9999-12-31 23:59:59.999");
        System.out.println(t0);
        System.out.println(t1);
        System.out.println(t2);
        System.out.println(t3);
        System.out.println(t3.getTime());
    }
    
    @Test
    public void subArrayDebug() {
        int[] abc = new int[]{1, 2, 3, 4, 5};
        int[] a = Arrays.copyOf(abc, 3);
        System.out.println(Arrays.stream(a).boxed().toList());
    }
    
    @Test
    public void streamTest() {
        List<ClassImplInterface> cList = List.of(new ClassImplInterface(1), new ClassImplInterface(2), new ClassImplInterface(3));
        List<Integer> commonFields = cList.stream().map(c -> c.getCommonField()).toList();
        System.out.println(commonFields);
    }
    
    @Test
    public void concreteDateType() {
        Date date = Date.valueOf("1996-10-10");
        Date lateDate = new Date(date.getTime() + 1);
        System.out.println(lateDate.compareTo(date));
    }
    
    @Test
    public void performanceOfToListMethods() {
        long stime = 0;
        long etime = 0;
        int length = 10000000;
        int[] bigArray = new int[length];
        for (int i = 0; i < length; i++) {
            bigArray[i] = i;
        }
        // 1. Arrays.asList() - wrong
        // stime = System.currentTimeMillis();
        // List arrayArray = Arrays.asList(bigArray);
        // System.out.println(arrayArray.get(1000));
        // System.out.println(arrayArray.get(1000000));
        // etime = System.currentTimeMillis();
        // System.out.printf("Arrays.asList() cost %d ms\n", etime - stime);
        
        // 2. new ArrayList(Arrays.asList()) - wrong
        
        // 3. Collections.addAll() - wrong
        // stime = System.currentTimeMillis();
        // List<Integer> collectionArray = new ArrayList<>(bigArray.length);
        // Collections.addAll(collectionArray, bigArray);
        // etime = System.currentTimeMillis();
        // System.out.printf("Collections.addAll() cost %d ms\n", etime - stime);
        
        // 4. stream
        stime = System.currentTimeMillis();
        List<Integer> streamArray = Arrays.stream(bigArray).boxed().toList();
        System.out.println(streamArray.get(1000));
        System.out.println(streamArray.get(1000000));
        etime = System.currentTimeMillis();
        System.out.printf("stream cost %d ms\n", etime - stime);
        
    }
    
    @Test
    public void timelineIteratorDebug() {
        String lineitembihFileName;
        String tableName;
        List<Pair<String, Class<?>>> columnList;
        Triple<String, String, String> activeTIme;
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
        activeTIme = Triple.of("activeTime", "activeTimeBegin", "activeTimeEnd");
        ColumnStoreTable lineitembih = new ColumnStoreTable(
            lineitembihFileName, 
            new TableSchema(columnList), 
            tableName,
            List.of(activeTIme)
        );
        TimelineIndex lineitembihIndex = new TimelineIndex(
            lineitembih, 
            lineitembih.getApplicationPeriod().getApplicationPeriodNames().get(0)
        );
                
        System.out.println("start");
        Iterator<RowIndexAtVersion> iterator = lineitembihIndex.iterator(0, Date.valueOf("1996-02-12"));
        while (iterator.hasNext()) {
            RowIndexAtVersion rowIndexPair = iterator.next();
        }
    }
    
    @Test
    public void hashmapPutTest() {
        Integer[] source = new Integer[]{1, 1, 2, 3, 3,3};
        HashMap<Integer, Integer> target = new HashMap<>();
        int count = 0;
        for (Integer i : source) {
            target.put(i, count++);
        }
        System.out.println(target);
    }

    @Test
    public void treeMapTest() {
        // TreeMap<Integer, Integer> treeMap = new TreeMap<>();
        // treeMap.put(1, 1);
        // treeMap.put(2, 2);
        // treeMap.put(4, 9);
        // treeMap.put(5, 11);
        // treeMap.put(3, 5);
        // treeMap.put(1, 14);
        
        // for (Entry<Integer, Integer> entry : treeMap.entrySet()) {
        //     System.out.println(entry.getKey() + " " + entry.getValue());
        // }
        
        TreeMap<Pair<Integer, Integer>, Boolean> complexTreeMap = new TreeMap<>(
            (a, b) -> { 
                return a.getLeft() - b.getLeft() == 0 ?
                    a.getRight() - b.getRight() : a.getLeft() - b.getLeft();
            }
        ); // Key小值在前，大值在后
        
        complexTreeMap.put(Pair.of(1, 1), false);
        complexTreeMap.put(Pair.of(2, 2), false);
        complexTreeMap.put(Pair.of(2, 4), false);
        complexTreeMap.put(Pair.of(5, 9), false);
        complexTreeMap.put(Pair.of(4, 14), false);
        
        for (Pair<Integer, Integer> k : complexTreeMap.navigableKeySet()) {
            System.out.println(k);
        }
    }

    @Test
    public void interfaceInheritTest() {
        InterfaceForTest abc = new ClassImplInterface(1000);
        AbstractClass wasd = new ClassImplInterface(10000);
        System.out.println(abc.getClass().getName());
        System.out.println(wasd.getClass().getName());
        System.out.println(wasd.getCommonField());
    }
    public interface InterfaceForTest {
        
    }
    public abstract class AbstractClass {
        protected int commonField;
        
        public AbstractClass(int commonField) {
            this.commonField = commonField;
        }
        
        public abstract int getCommonField();
    }
    public class ClassImplInterface extends AbstractClass implements InterfaceForTest{
        public ClassImplInterface(int commonField) {
            super(commonField);
        }
        private static Integer abc = return4();
        static {
            System.out.println("execute abc = 3");
            abc = 3;
        }
        private static Integer return4() {
            System.out.println("execute return4");
            return 4;
        }
        @Override
        public int getCommonField() {
            return commonField;
        }
    }

    @Test
    public void twoDimonsionsListAndArrayTest() {
        List<List<Integer>> abc = List.of(
            List.of(1, 2, 3), 
            List.of(4, 5, 6), 
            List.of(7, 8, 9));

        Integer[][] wasd = new Integer[][]{{1,2,3},{4,5,6},{7,8,9}};

        SizeOf.skipStaticField(true);
        System.out.println(SizeOf.deepSizeOf(abc));
        System.out.println(SizeOf.deepSizeOf(wasd));
    }

    @Test
    public void arrayOutOfBounds() {
        arrayBound(1, 2, 3);

    }
    public void arrayBound(Object ... objs) {
        if (objs.length != 4) throw new ArrayIndexOutOfBoundsException();
    }

    @Test
    public void listCreateTest() {
        List<?>[] abc = new List[5];
        abc[0] = List.of(1, 2); // right way
        abc[1] = new ArrayList<>(); // right way
        
        // MethodAccess methodAccess = MethodAccess.get(ArrayList.class);// unsafe way
        // methodAccess.invoke(abc[1], "add", (Integer)1);// unsafe way 
        // methodAccess.invoke(abc[1], "add", (Long)2L);// unsafe way
        // 声明一个泛型类的包装类，在加入第一个元素时才明确该类具体类型
        // 该包装类中需要校验加入的元素是否与已有元素相同类型，提供具体类型的get函数
        // public class GenericArrayList {
        //     private ArrayList<?> list;
        // }
        // 方法可以，但是性能有问题
        // GenericArrayList::add cost 306 ms
        // ArrayList::add cost 199 ms
        
        abc[2] = List.of("String", "wasd"); // right way
        System.out.println(abc[0]);
        System.out.println(abc[1]);
        System.out.println(abc[1].get(0).getClass());
        System.out.println(abc[2]);
        // abc[3] = new ArrayList<>(); // wrong way
        // abc[3].add("wrong"); // wrong way
        // abc[3].add((Object)60); // wrong way

        // List<Object>[] wasd = new List<Object>[5]; // wrong way
        List<Object>[] wasd = new List[5]; // unsafe way
        List<Object>[] yogurt = (List<Object>[])Array.newInstance(List.class, 5); // unsafe way
    }

    @Test
    public void arrayListGetTest() {
        List<Integer> abc = List.of(1, 2, 3);
        System.out.println(abc.contains(4));
    }

    @Test
    public void sizeofArrays() {
        SizeOf.skipStaticField(true);
        System.out.println(SizeOf.deepSizeOf(new int[]{1, 2, 3, 4, 5}));
        System.out.println(SizeOf.deepSizeOf(new Integer[]{1, 2, 3, 4, 5}));
        System.out.println(SizeOf.deepSizeOf(new long[]{1L, 2L, 3L, 4L, 5L}));
        System.out.println(SizeOf.deepSizeOf(new Long[]{1L, 2L, 3L, 4L, 5L}));
    }
    
    @Test
    public void bitsetTest() {
        BitSet abc = new BitSet();
        abc.set(20);
        abc.flip(25);
        abc.flip(22);
        abc.set(25);
        System.out.println(abc.nextSetBit(26));
        // for (Byte bitByte : abc.toByteArray()) {
        //     System.out.printf("%d ", bitByte);
        // }
        // System.out.println();
        // for (long longbit : abc.toByteArray()) {
        //     System.out.printf("%d ", longbit);
        // }
        // System.out.println();
        // for (int intbit : abc.stream().toArray()) {
        //     System.out.printf("%d ", intbit);
        // }
        // System.out.println();
        System.out.println(abc.cardinality());
    }

}
