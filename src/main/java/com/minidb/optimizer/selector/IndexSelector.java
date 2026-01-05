package com.minidb.optimizer.selector;

import com.minidb.optimizer.cost.CostModel;
import com.minidb.optimizer.index.IndexMetadata;
import com.minidb.optimizer.stats.StatisticsCollector;
import com.minidb.optimizer.stats.TableStatistics;
import com.minidb.parser.ast.Expression;
import com.minidb.parser.ast.SelectStatement;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * 索引选择器
 *
 * 基于成本模型选择最优的索引执行查询
 *
 * 选择策略:
 * 1. 收集所有可用的索引
 * 2. 估算每个索引的成本
 * 3. 选择成本最低的索引
 * 4. 如果没有索引或索引成本太高，使用全表扫描
 *
 * 对应八股文知识点:
 * ✅ MySQL 如何选择索引
 * ✅ 索引失效的场景
 * ✅ 最左前缀原则
 * ✅ 覆盖索引优化
 * ✅ 索引下推
 *
 * @author Mini-MySQL
 */
@Slf4j
public class IndexSelector {

    /**
     * 成本模型
     */
    private final CostModel costModel;

    /**
     * 统计信息收集器
     */
    private final StatisticsCollector statsCollector;

    public IndexSelector(CostModel costModel, StatisticsCollector statsCollector) {
        this.costModel = costModel;
        this.statsCollector = statsCollector;
    }

    /**
     * 为 SELECT 查询选择最优索引
     *
     * @param selectStmt SELECT 语句
     * @param availableIndexes 可用的索引列表
     * @return 索引选择结果
     */
    public IndexSelectionResult selectIndex(SelectStatement selectStmt,
                                             List<IndexMetadata> availableIndexes) {
        String tableName = selectStmt.getTableName();
        log.info("Selecting index for query on table: {}", tableName);

        // 获取表统计信息
        TableStatistics stats = statsCollector.getTableStats(tableName);
        if (stats == null) {
            log.warn("No statistics found for table: {}", tableName);
            return createTableScanResult(tableName);
        }

        // 计算全表扫描成本作为基准
        double tableScanCost = costModel.estimateTableScanCost(stats);
        log.debug("Table scan cost: {}", tableScanCost);

        // 提取 WHERE 条件中的列
        List<String> whereColumns = extractWhereColumns(selectStmt.getWhereCondition());
        if (whereColumns.isEmpty()) {
            log.debug("No WHERE clause, using table scan");
            return createTableScanResult(tableName, tableScanCost);
        }

        // 评估每个索引的成本
        IndexSelectionResult bestResult = null;
        double bestCost = tableScanCost;

        for (IndexMetadata index : availableIndexes) {
            // 检查索引是否可用（最左前缀原则）
            if (!index.canUseFor(whereColumns)) {
                log.debug("Index {} cannot be used (violates left-most prefix rule)",
                        index.getIndexName());
                continue;
            }

            // 估算过滤率
            double filterRate = estimateFilterRate(tableName, whereColumns);

            // 检查是否为覆盖索引
            List<String> selectColumns = extractSelectColumns(selectStmt);
            boolean isCovering = index.covers(selectColumns);

            // 计算索引成本
            double indexCost;
            if (index.isClustered()) {
                // 聚簇索引不需要回表
                indexCost = costModel.estimateIndexScanCost(stats, index, filterRate);
            } else {
                // 二级索引可能需要回表
                indexCost = costModel.estimateSecondaryIndexCost(
                        stats, index, filterRate, !isCovering);
            }

            log.debug("Index {} cost: {} (covering: {})",
                    index.getIndexName(), indexCost, isCovering);

            // 选择成本最低的索引
            if (indexCost < bestCost) {
                bestCost = indexCost;
                bestResult = createIndexScanResult(
                        tableName, index, indexCost, filterRate, isCovering);
            }
        }

        // 如果没有找到更优的索引，使用全表扫描
        if (bestResult == null) {
            log.info("No suitable index found, using table scan");
            return createTableScanResult(tableName, tableScanCost);
        }

        log.info("Selected index: {} (cost: {} vs table scan: {})",
                bestResult.getSelectedIndex().getIndexName(),
                bestResult.getEstimatedCost(), tableScanCost);

        return bestResult;
    }

    /**
     * 提取 WHERE 子句中的列名
     *
     * @param whereExpr WHERE 表达式
     * @return 列名列表
     */
    private List<String> extractWhereColumns(Expression whereExpr) {
        List<String> columns = new ArrayList<>();
        if (whereExpr == null) {
            return columns;
        }

        extractColumnsRecursive(whereExpr, columns);
        return columns;
    }

    /**
     * 递归提取表达式中的列名
     */
    private void extractColumnsRecursive(Expression expr, List<String> columns) {
        if (expr == null) {
            return;
        }

        if (expr instanceof Expression.ColumnReference) {
            String columnName = ((Expression.ColumnReference) expr).getColumnName();
            if (!columns.contains(columnName)) {
                columns.add(columnName);
            }
        } else if (expr instanceof Expression.BinaryExpression) {
            Expression.BinaryExpression binExpr = (Expression.BinaryExpression) expr;
            extractColumnsRecursive(binExpr.getLeft(), columns);
            extractColumnsRecursive(binExpr.getRight(), columns);
        } else if (expr instanceof Expression.UnaryExpression) {
            Expression.UnaryExpression unaryExpr = (Expression.UnaryExpression) expr;
            extractColumnsRecursive(unaryExpr.getOperand(), columns);
        } else if (expr instanceof Expression.InExpression) {
            String columnName = ((Expression.InExpression) expr).getColumnName();
            if (!columns.contains(columnName)) {
                columns.add(columnName);
            }
        } else if (expr instanceof Expression.BetweenExpression) {
            String columnName = ((Expression.BetweenExpression) expr).getColumnName();
            if (!columns.contains(columnName)) {
                columns.add(columnName);
            }
        }
    }

    /**
     * 提取 SELECT 子句中的列名
     *
     * @param selectStmt SELECT 语句
     * @return 列名列表
     */
    private List<String> extractSelectColumns(SelectStatement selectStmt) {
        if (selectStmt.isSelectAll()) {
            return Collections.emptyList(); // SELECT * 不能使用覆盖索引
        }

        List<String> columns = new ArrayList<>();
        for (SelectStatement.SelectElement element : selectStmt.getSelectElements()) {
            columns.add(element.getColumnName());
        }
        return columns;
    }

    /**
     * 估算过滤率
     *
     * @param tableName 表名
     * @param whereColumns WHERE 子句中的列
     * @return 过滤率 [0, 1]
     */
    private double estimateFilterRate(String tableName, List<String> whereColumns) {
        if (whereColumns.isEmpty()) {
            return 1.0;
        }

        // 简化实现: 使用第一个列的选择性
        String firstColumn = whereColumns.get(0);
        double selectivity = statsCollector.getSelectivity(tableName, firstColumn);

        // 假设等值查询
        return selectivity > 0 ? (1.0 / selectivity) : 0.1;
    }

    /**
     * 创建全表扫描结果
     */
    private IndexSelectionResult createTableScanResult(String tableName) {
        TableStatistics stats = statsCollector.getTableStats(tableName);
        long estimatedRows = stats != null ? stats.getRowCount() : 0;
        double cost = stats != null ? costModel.estimateTableScanCost(stats) : 0.0;
        return createTableScanResult(tableName, cost, estimatedRows);
    }

    private IndexSelectionResult createTableScanResult(String tableName, double cost) {
        TableStatistics stats = statsCollector.getTableStats(tableName);
        long estimatedRows = stats != null ? stats.getRowCount() : 0;
        return createTableScanResult(tableName, cost, estimatedRows);
    }

    private IndexSelectionResult createTableScanResult(String tableName, double cost, long rows) {
        IndexSelectionResult result = new IndexSelectionResult();
        result.setTableName(tableName);
        result.setScanType(ScanType.TABLE_SCAN);
        result.setEstimatedCost(cost);
        result.setEstimatedRows(rows);
        result.setNeedLookup(false);
        result.setCoveringIndex(false);
        return result;
    }

    /**
     * 创建索引扫描结果
     */
    private IndexSelectionResult createIndexScanResult(String tableName,
                                                        IndexMetadata index,
                                                        double cost,
                                                        double filterRate,
                                                        boolean isCovering) {
        IndexSelectionResult result = new IndexSelectionResult();
        result.setTableName(tableName);
        result.setSelectedIndex(index);
        result.setScanType(index.isClustered() ?
                ScanType.CLUSTERED_INDEX_SCAN : ScanType.SECONDARY_INDEX_SCAN);
        result.setEstimatedCost(cost);

        TableStatistics stats = statsCollector.getTableStats(tableName);
        result.setEstimatedRows((long) (stats.getRowCount() * filterRate));
        result.setNeedLookup(!index.isClustered() && !isCovering);
        result.setCoveringIndex(isCovering);

        return result;
    }

    /**
     * 扫描类型
     */
    public enum ScanType {
        TABLE_SCAN,              // 全表扫描
        CLUSTERED_INDEX_SCAN,    // 聚簇索引扫描
        SECONDARY_INDEX_SCAN     // 二级索引扫描
    }

    /**
     * 索引选择结果
     */
    @Data
    public static class IndexSelectionResult {
        /**
         * 表名
         */
        private String tableName;

        /**
         * 扫描类型
         */
        private ScanType scanType;

        /**
         * 选中的索引（如果使用索引）
         */
        private IndexMetadata selectedIndex;

        /**
         * 估算的成本
         */
        private double estimatedCost;

        /**
         * 估算的返回行数
         */
        private long estimatedRows;

        /**
         * 是否需要回表
         */
        private boolean needLookup;

        /**
         * 是否为覆盖索引
         */
        private boolean coveringIndex;

        @Override
        public String toString() {
            if (scanType == ScanType.TABLE_SCAN) {
                return String.format("TableScan{table=%s, cost=%.2f}",
                        tableName, estimatedCost);
            } else {
                return String.format("%s{table=%s, index=%s, cost=%.2f, rows=%d, lookup=%b, covering=%b}",
                        scanType, tableName,
                        selectedIndex != null ? selectedIndex.getIndexName() : "null",
                        estimatedCost, estimatedRows, needLookup, coveringIndex);
            }
        }
    }
}
