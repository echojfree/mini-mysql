package com.minidb.optimizer.stats;

import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * 统计信息收集器
 *
 * 负责收集和维护表、索引的统计信息，供查询优化器使用
 *
 * 核心统计信息:
 * 1. 表行数: 估算全表扫描成本
 * 2. 索引基数: 估算索引选择性
 * 3. 数据分布: 直方图（简化版）
 *
 * 对应八股文知识点:
 * ✅ MySQL 统计信息的作用
 * ✅ ANALYZE TABLE 的工作原理
 * ✅ 为什么要定期更新统计信息
 * ✅ 统计信息不准确会导致什么问题
 *
 * @author Mini-MySQL
 */
@Slf4j
public class StatisticsCollector {

    /**
     * 表名 -> 表统计信息
     */
    private final Map<String, TableStatistics> tableStats;

    /**
     * (表名, 列名) -> 列基数
     */
    private final Map<String, Map<String, Long>> columnCardinality;

    /**
     * 页面大小（字节）
     * InnoDB 默认 16KB
     */
    private static final int PAGE_SIZE = 16 * 1024;

    public StatisticsCollector() {
        this.tableStats = new HashMap<>();
        this.columnCardinality = new HashMap<>();
    }

    /**
     * 收集表的统计信息
     *
     * 模拟 MySQL 的 ANALYZE TABLE 命令
     *
     * @param tableName 表名
     * @param data 表中的所有行数据
     */
    public void analyzeTable(String tableName, List<Map<String, Object>> data) {
        log.info("Analyzing table: {}", tableName);

        TableStatistics stats = new TableStatistics();
        stats.setTableName(tableName);
        stats.setRowCount(data.size());
        stats.setLastUpdateTime(System.currentTimeMillis());

        if (data.isEmpty()) {
            stats.setAvgRowLength(0);
            stats.setDataFileSize(0);
            stats.setPageCount(0);
        } else {
            // 计算平均行长度
            long totalBytes = 0;
            for (Map<String, Object> row : data) {
                totalBytes += estimateRowSize(row);
            }
            stats.setAvgRowLength((int) (totalBytes / data.size()));

            // 计算数据文件大小和页面数
            stats.setDataFileSize(totalBytes);
            stats.setPageCount(stats.calculatePageCount(PAGE_SIZE));
        }

        tableStats.put(tableName, stats);

        log.info("Table analysis complete: {}", stats);
    }

    /**
     * 收集列的基数（不同值的数量）
     *
     * 八股文:
     * - 基数越高，索引选择性越好
     * - 性别列基数为 2，选择性差
     * - 用户ID列基数等于行数，选择性最好
     *
     * @param tableName 表名
     * @param columnName 列名
     * @param data 表中的所有行数据
     */
    public void analyzeColumn(String tableName, String columnName,
                               List<Map<String, Object>> data) {
        log.debug("Analyzing column: {}.{}", tableName, columnName);

        Set<Object> distinctValues = new HashSet<>();
        for (Map<String, Object> row : data) {
            Object value = row.get(columnName);
            if (value != null) {
                distinctValues.add(value);
            }
        }

        long cardinality = distinctValues.size();
        columnCardinality
                .computeIfAbsent(tableName, k -> new HashMap<>())
                .put(columnName, cardinality);

        log.debug("Column cardinality: {}.{} = {}", tableName, columnName, cardinality);
    }

    /**
     * 获取表的统计信息
     *
     * @param tableName 表名
     * @return 表统计信息
     */
    public TableStatistics getTableStats(String tableName) {
        return tableStats.get(tableName);
    }

    /**
     * 获取列的基数
     *
     * @param tableName 表名
     * @param columnName 列名
     * @return 基数，如果未收集返回 0
     */
    public long getColumnCardinality(String tableName, String columnName) {
        Map<String, Long> columns = columnCardinality.get(tableName);
        if (columns == null) {
            return 0;
        }
        return columns.getOrDefault(columnName, 0L);
    }

    /**
     * 计算列的选择性
     *
     * 选择性 = 基数 / 总行数
     * 取值范围 [0, 1]
     *
     * 八股文:
     * - 选择性越接近 1，索引效果越好
     * - 选择性 < 0.1 通常不建议建索引
     *
     * @param tableName 表名
     * @param columnName 列名
     * @return 选择性
     */
    public double getSelectivity(String tableName, String columnName) {
        TableStatistics stats = tableStats.get(tableName);
        if (stats == null || stats.getRowCount() == 0) {
            return 0.0;
        }

        long cardinality = getColumnCardinality(tableName, columnName);
        return (double) cardinality / stats.getRowCount();
    }

    /**
     * 估算条件的过滤率
     *
     * 过滤率 = 满足条件的行数 / 总行数
     *
     * 简化实现:
     * - 等值条件: 1 / 基数
     * - 范围条件: 0.33 (经验值)
     * - IS NULL: 0.1 (经验值)
     *
     * 八股文: MySQL 优化器使用统计信息估算过滤率
     *
     * @param tableName 表名
     * @param columnName 列名
     * @param operator 操作符 (=, >, <, etc.)
     * @return 过滤率 [0, 1]
     */
    public double estimateFilterRate(String tableName, String columnName, String operator) {
        long cardinality = getColumnCardinality(tableName, columnName);
        if (cardinality == 0) {
            return 0.5; // 默认值
        }

        switch (operator) {
            case "=":
            case "==":
                // 等值查询: 假设均匀分布
                return 1.0 / cardinality;

            case ">":
            case "<":
            case ">=":
            case "<=":
            case "BETWEEN":
                // 范围查询: 经验值 33%
                return 0.33;

            case "IN":
                // IN 查询: 根据值的数量估算
                return Math.min(5.0 / cardinality, 0.5);

            case "IS NULL":
                // NULL 查询: 经验值 10%
                return 0.1;

            case "LIKE":
                // LIKE 查询: 经验值 25%
                return 0.25;

            default:
                return 0.5; // 未知操作符，使用默认值
        }
    }

    /**
     * 估算行的字节大小
     *
     * 简化实现，实际 MySQL 会考虑列类型、变长字段等
     *
     * @param row 行数据
     * @return 估算的字节数
     */
    private int estimateRowSize(Map<String, Object> row) {
        int size = 0;
        for (Object value : row.values()) {
            if (value == null) {
                size += 1; // NULL 标记
            } else if (value instanceof String) {
                size += ((String) value).length();
            } else if (value instanceof Integer) {
                size += 4;
            } else if (value instanceof Long) {
                size += 8;
            } else if (value instanceof Double || value instanceof Float) {
                size += 8;
            } else {
                size += 8; // 默认大小
            }
        }
        return size;
    }

    /**
     * 清空所有统计信息
     */
    public void clear() {
        tableStats.clear();
        columnCardinality.clear();
    }

    /**
     * 获取所有表的统计信息
     *
     * @return 表名 -> 统计信息的映射
     */
    public Map<String, TableStatistics> getAllTableStats() {
        return new HashMap<>(tableStats);
    }
}
