package com.minidb.executor.operator;

import com.minidb.executor.Operator;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * 排序算子
 *
 * 功能: ORDER BY 排序
 *
 * 工作原理:
 * 1. 从子算子读取所有数据（物化）
 * 2. 在内存中排序
 * 3. 逐行返回排序后的结果
 *
 * 对应八股文知识点:
 * ✅ ORDER BY 的执行过程
 * ✅ 什么是 filesort
 * ✅ 如何避免 filesort（使用索引排序）
 * ✅ 内存排序 vs 外部排序
 *
 * @author Mini-MySQL
 */
@Slf4j
public class SortOperator implements Operator {

    /**
     * 子算子（数据源）
     */
    private final Operator child;

    /**
     * 排序列名
     */
    private final List<String> orderByColumns;

    /**
     * 是否降序
     * 与 orderByColumns 一一对应
     */
    private final List<Boolean> descending;

    /**
     * 排序后的数据
     * 需要物化（Materialization）
     */
    private List<Map<String, Object>> sortedData;

    /**
     * 当前迭代器
     */
    private Iterator<Map<String, Object>> iterator;

    /**
     * 统计信息
     */
    private int sortedRows = 0;

    public SortOperator(Operator child, List<String> orderByColumns,
                       List<Boolean> descending) {
        this.child = child;
        this.orderByColumns = orderByColumns;
        this.descending = descending;
    }

    @Override
    public void open() throws Exception {
        log.debug("Opening Sort: columns={}, desc={}", orderByColumns, descending);
        child.open();

        // 读取所有数据
        sortedData = new ArrayList<>();
        Map<String, Object> row;
        while ((row = child.next()) != null) {
            sortedData.add(row);
        }

        log.debug("Sort collected {} rows, starting sort...", sortedData.size());

        // 排序
        long startTime = System.currentTimeMillis();
        sortedData.sort(createComparator());
        long sortTime = System.currentTimeMillis() - startTime;

        log.debug("Sort finished in {} ms", sortTime);

        // 创建迭代器
        iterator = sortedData.iterator();
        sortedRows = 0;
    }

    @Override
    public Map<String, Object> next() throws Exception {
        if (iterator == null) {
            throw new IllegalStateException("Operator not opened");
        }

        if (iterator.hasNext()) {
            sortedRows++;
            return iterator.next();
        }

        return null;
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing Sort, returned {} rows", sortedRows);
        child.close();
        sortedData = null;
        iterator = null;
    }

    @Override
    public String getOperatorType() {
        return "Sort(" + orderByColumns + ")";
    }

    /**
     * 创建比较器
     */
    private Comparator<Map<String, Object>> createComparator() {
        return (row1, row2) -> {
            for (int i = 0; i < orderByColumns.size(); i++) {
                String column = orderByColumns.get(i);
                boolean desc = i < descending.size() ? descending.get(i) : false;

                Object val1 = row1.get(column);
                Object val2 = row2.get(column);

                int cmp = compareValues(val1, val2);

                if (cmp != 0) {
                    return desc ? -cmp : cmp;
                }
            }
            return 0;
        };
    }

    /**
     * 比较两个值
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private int compareValues(Object val1, Object val2) {
        if (val1 == null && val2 == null) {
            return 0;
        }
        if (val1 == null) {
            return -1;
        }
        if (val2 == null) {
            return 1;
        }

        // 尝试作为 Comparable 比较
        if (val1 instanceof Comparable && val2 instanceof Comparable) {
            try {
                return ((Comparable) val1).compareTo(val2);
            } catch (ClassCastException e) {
                // 类型不匹配，使用字符串比较
                return val1.toString().compareTo(val2.toString());
            }
        }

        // 默认使用字符串比较
        return val1.toString().compareTo(val2.toString());
    }

    public int getSortedRows() {
        return sortedRows;
    }
}
