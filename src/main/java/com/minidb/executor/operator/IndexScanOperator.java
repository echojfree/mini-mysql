package com.minidb.executor.operator;

import com.minidb.executor.Operator;
import com.minidb.storage.index.BPlusTree;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * 索引扫描算子
 *
 * 功能: 使用索引进行范围扫描或精确查询
 *
 * 工作原理:
 * 1. 使用 B+ 树索引定位到起始位置
 * 2. 利用叶子节点链表顺序扫描
 * 3. 返回满足条件的记录
 *
 * 对应八股文知识点:
 * ✅ 索引扫描 vs 全表扫描
 * ✅ 范围查询的实现
 * ✅ 索引覆盖优化
 * ✅ 回表查询的代价
 * ✅ 最左前缀原则的应用
 *
 * @author Mini-MySQL
 */
@Slf4j
public class IndexScanOperator implements Operator {

    /**
     * 表名
     */
    private final String tableName;

    /**
     * B+ 树索引
     */
    @SuppressWarnings("rawtypes")
    private final BPlusTree index;

    /**
     * 扫描范围的起始键（包含）
     * null 表示从最小值开始
     */
    private final Comparable<?> startKey;

    /**
     * 扫描范围的结束键（包含）
     * null 表示到最大值结束
     */
    private final Comparable<?> endKey;

    /**
     * 是否为精确查询（等值查询）
     */
    private final boolean exactMatch;

    /**
     * 当前迭代器
     * 遍历索引返回的结果
     */
    private Iterator<Map<String, Object>> iterator;

    /**
     * 扫描到的行数统计
     */
    private int scannedRows = 0;

    /**
     * 构造函数 - 范围扫描
     *
     * @param tableName 表名
     * @param index B+ 树索引
     * @param startKey 起始键（null 表示无下界）
     * @param endKey 结束键（null 表示无上界）
     */
    @SuppressWarnings("rawtypes")
    public IndexScanOperator(String tableName, BPlusTree index,
                            Comparable<?> startKey, Comparable<?> endKey) {
        this.tableName = tableName;
        this.index = index;
        this.startKey = startKey;
        this.endKey = endKey;
        this.exactMatch = false;
    }

    /**
     * 构造函数 - 精确查询（等值查询）
     *
     * @param tableName 表名
     * @param index B+ 树索引
     * @param key 查询键值
     */
    @SuppressWarnings("rawtypes")
    public IndexScanOperator(String tableName, BPlusTree index, Comparable<?> key) {
        this.tableName = tableName;
        this.index = index;
        this.startKey = key;
        this.endKey = key;
        this.exactMatch = true;
    }

    @Override
    public void open() throws Exception {
        log.debug("Opening IndexScan on table: {}, range: [{}, {}], exact: {}",
                tableName, startKey, endKey, exactMatch);

        // 执行索引范围查询
        List<Map<String, Object>> results = performIndexScan();

        // 创建迭代器
        iterator = results.iterator();
        scannedRows = 0;
    }

    @Override
    public Map<String, Object> next() throws Exception {
        if (iterator == null) {
            throw new IllegalStateException("Operator not opened");
        }

        if (iterator.hasNext()) {
            scannedRows++;
            Map<String, Object> row = iterator.next();
            log.trace("IndexScan returned row {}: {}", scannedRows, row);
            return row;
        }

        log.debug("IndexScan finished, scanned {} rows", scannedRows);
        return null;
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing IndexScan, total scanned: {} rows", scannedRows);
        iterator = null;
    }

    @Override
    public String getOperatorType() {
        if (exactMatch) {
            return "IndexScan(=" + startKey + ")";
        } else {
            return "IndexScan([" + startKey + ", " + endKey + "])";
        }
    }

    /**
     * 执行索引扫描
     *
     * 实际场景中:
     * 1. 如果是精确查询，调用 index.search(key)
     * 2. 如果是范围查询，调用 index.rangeSearch(start, end)
     * 3. 如果是二级索引，可能需要回表查询
     *
     * 简化实现:
     * 这里假设索引已经包含完整的行数据（类似聚簇索引）
     * 实际中二级索引只包含主键，需要回表到聚簇索引获取完整数据
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> performIndexScan() {
        List<Map<String, Object>> results = new ArrayList<>();

        if (exactMatch) {
            // 精确查询: 等值查找
            log.debug("Performing exact match for key: {}", startKey);

            Object value = index.search(startKey);
            if (value != null) {
                if (value instanceof Map) {
                    results.add((Map<String, Object>) value);
                } else {
                    log.warn("Index value is not a Map: {}", value.getClass());
                }
            }

        } else {
            // 范围查询
            log.debug("Performing range scan: [{}, {}]", startKey, endKey);

            @SuppressWarnings("unchecked")
            List<BPlusTree.Entry<Comparable<?>, Object>> rangeResults =
                (List<BPlusTree.Entry<Comparable<?>, Object>>) (List<?>) index.rangeSearch(startKey, endKey);

            for (BPlusTree.Entry<?, ?> entry : rangeResults) {
                Object value = entry.getValue();
                if (value instanceof Map) {
                    results.add((Map<String, Object>) value);
                } else {
                    log.warn("Index value is not a Map: {}", value.getClass());
                }
            }
        }

        log.debug("IndexScan found {} rows", results.size());
        return results;
    }

    /**
     * 获取扫描的行数（用于统计）
     */
    public int getScannedRows() {
        return scannedRows;
    }

    /**
     * 判断是否为精确查询
     */
    public boolean isExactMatch() {
        return exactMatch;
    }
}
