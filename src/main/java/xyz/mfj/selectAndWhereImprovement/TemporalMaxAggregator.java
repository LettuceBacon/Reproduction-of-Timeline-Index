package xyz.mfj.selectAndWhereImprovement;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xyz.mfj.operators.CompareOperator;
import xyz.mfj.utils.TypeUtil;

public class TemporalMaxAggregator implements Aggregator{
    private static Logger log = LoggerFactory.getLogger(TemporalMaxAggregator.class);
    
    private Class<?> type;
    private TopKSet topKSet; // 内部实现是一个从小到大排序的java.util.TreeMap
    private LinkedList<Object> validVector;
    private LinkedList<Object> deletedVector;
    private CompareOperator cmpr;
    
    public TemporalMaxAggregator(Class<?> type) {
        this.type = type;
        this.cmpr = TypeUtil.getCmprOperOfType(type);
        this.topKSet = new TopKSet(this.cmpr.getOper());
        this.validVector = new LinkedList<>();
        this.deletedVector = new LinkedList<>();
        this.topKSet.insert(TypeUtil.getMinOfType(type));
    }

    @Override
    public void collect(Object value, boolean flag) {
        if (flag == true) {
            if (topKSet.size() == 0 || cmpr.compare(value, topKSet.getMinValue()) >= 0) { // 大于或等于topk集最小值
                Object valueKPlusOne = topKSet.insert(value);
                if (valueKPlusOne != null) { // 插入topk集后多出一个
                    validVector.add(valueKPlusOne);
                }
            }
            else {
                validVector.add(value);
            }
        }
        else {
            if (topKSet.containsValue(value)) {
                topKSet.deleteAndGet(value);
                while (topKSet.size() <= 0) {
                    rebuildTopKSet();
                }
            }
            else {
                deletedVector.add(value);
            }
        }
    }

    @Override
    public Object aggregate() {
        return topKSet.getMaxValue();
    }
    
    private void rebuildTopKSet() {
        int validVectorSize = validVector.size();
        int deletedVectorSize = deletedVector.size();
        for (int i = 0; i < validVectorSize; i++) {
            Object valueKPlusOne = topKSet.insert(validVector.remove());
            if (valueKPlusOne != null) { // 插入topk集后多出一个
                validVector.add(valueKPlusOne);
            }
        }
        for (int i = 0; i < deletedVectorSize; i++) {
            Object value = deletedVector.remove();
            if (topKSet.containsValue(value)) {
                topKSet.deleteAndGet(value);
            }
            else {
                deletedVector.add(value);
            }
        }
    }
    
    
    private class TopKSet {
        private TreeMap<Object, Integer> ascendOrderTree;
        private int k = 100;
        private int size = 0;
        
        public TopKSet(Comparator<Object> cmpr, int k) {
            this.ascendOrderTree = new TreeMap<>(cmpr);
            this.k = k;
        }
        
        public TopKSet(Comparator<Object> cmpr) {
            this.ascendOrderTree = new TreeMap<>(cmpr);
        }
        
        public Object getMinValue() {
            return ascendOrderTree.firstKey();
        }
        
        public Object getMaxValue() {
            return ascendOrderTree.lastKey();
        }
        
        public Object insert(Object value) {
            Integer oldCount = ascendOrderTree.get(value);
            if (oldCount == null) { // 树中没有对应的值
                ascendOrderTree.put(value, 1);
            }
            else {
                ascendOrderTree.put(value, oldCount + 1);
            }
            size++;
            if (size > k) { // 插入topk集后多出一个，从树中拿出一个最小值并返回
                return deleteAndGet(getMinValue());
            }
            else {
                return null;
            }
        }
        
        public boolean containsValue(Object value) {
            return ascendOrderTree.containsKey(value);
        }
        
        public Object deleteAndGet(Object value) {
            Integer oldCount = ascendOrderTree.get(value);
            if (oldCount == null) { 
                log.error("Deleting an unexisted value from topK set!");
                return null;
            }
            else {
                if (oldCount - 1 > 0) {
                    ascendOrderTree.put(value, oldCount - 1);
                }
                else {
                    ascendOrderTree.remove(value);
                }
                size--;
                return value;
            }
        }
        
        public int size() {
            return size;
        }
    }
}
