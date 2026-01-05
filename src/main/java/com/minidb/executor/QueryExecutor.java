package com.minidb.executor;

import com.minidb.executor.operator.*;
import com.minidb.parser.ast.SelectStatement;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.function.Predicate;

/**
 * 查询执行器
 *
 * 功能: 根据 SQL AST 构建执行计划并执行
 *
 * 执行流程:
 * 1. 根据 SQL 构建算子树（Operator Tree）
 * 2. 调用 open() 初始化
 * 3. 循环调用 next() 获取结果
 * 4. 调用 close() 清理资源
 *
 * 对应八股文知识点:
 * ✅ SQL 的执行流程
 * ✅ 执行计划的生成
 * ✅ 火山模型的实际应用
 *
 * @author Mini-MySQL
 */
@Slf4j
public class QueryExecutor {

    /**
     * 执行 SELECT 查询
     *
     * @param selectStmt SELECT 语句 AST
     * @param tableData 表数据（简化实现，实际应从存储引擎获取）
     * @return 查询结果集
     */
    public List<Map<String, Object>> execute(SelectStatement selectStmt,
                                               Map<String, List<Map<String, Object>>> tableData)
            throws Exception {
        log.info("Executing query on table: {}", selectStmt.getTableName());

        // 构建执行计划
        Operator plan = buildExecutionPlan(selectStmt, tableData);

        // 执行查询
        List<Map<String, Object>> results = new ArrayList<>();

        try {
            // 1. 初始化
            plan.open();

            // 2. 获取结果
            Map<String, Object> row;
            while ((row = plan.next()) != null) {
                results.add(new HashMap<>(row)); // 复制一份数据
            }

            log.info("Query finished, returned {} rows", results.size());

        } finally {
            // 3. 清理资源
            plan.close();
        }

        return results;
    }

    /**
     * 构建执行计划
     *
     * 执行计划是一棵算子树:
     * - 叶子节点: TableScan (数据源)
     * - 中间节点: Filter, Project, Sort (数据处理)
     * - 根节点: Limit (结果限制)
     *
     * 构建顺序（从下到上）:
     * 1. TableScan - 扫描表
     * 2. Filter - 应用 WHERE 条件
     * 3. Sort - 应用 ORDER BY
     * 4. Project - 应用 SELECT 列
     * 5. Limit - 应用 LIMIT
     */
    private Operator buildExecutionPlan(SelectStatement selectStmt,
                                         Map<String, List<Map<String, Object>>> tableData) {
        String tableName = selectStmt.getTableName();

        // 获取表数据
        List<Map<String, Object>> data = tableData.get(tableName);
        if (data == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        // 1. TableScan - 全表扫描
        Operator plan = new TableScanOperator(tableName, data);
        log.debug("Added TableScan operator");

        // 2. Filter - WHERE 条件
        if (selectStmt.getWhereCondition() != null) {
            Predicate<Map<String, Object>> predicate = buildPredicate(selectStmt.getWhereCondition());
            String filterDesc = selectStmt.getWhereCondition().toString();
            plan = new FilterOperator(plan, predicate, filterDesc);
            log.debug("Added Filter operator: {}", filterDesc);
        }

        // 3. Sort - ORDER BY
        if (selectStmt.getOrderByElements() != null &&
                !selectStmt.getOrderByElements().isEmpty()) {
            List<String> orderColumns = new ArrayList<>();
            List<Boolean> descending = new ArrayList<>();

            for (SelectStatement.OrderByElement element : selectStmt.getOrderByElements()) {
                orderColumns.add(element.getColumnName());
                descending.add(element.isDescending());
            }

            plan = new SortOperator(plan, orderColumns, descending);
            log.debug("Added Sort operator: {}", orderColumns);
        }

        // 4. Project - SELECT 列
        if (selectStmt.isSelectAll()) {
            // SELECT * - 不需要投影
            log.debug("SELECT *, skipping Project operator");
        } else {
            List<String> selectColumns = new ArrayList<>();
            Map<String, String> aliases = new HashMap<>();

            for (SelectStatement.SelectElement element : selectStmt.getSelectElements()) {
                selectColumns.add(element.getColumnName());
                if (element.getAlias() != null) {
                    aliases.put(element.getColumnName(), element.getAlias());
                }
            }

            plan = new ProjectOperator(plan, selectColumns, aliases);
            log.debug("Added Project operator: {}", selectColumns);
        }

        // 5. Limit - LIMIT
        if (selectStmt.getLimit() != null) {
            plan = new LimitOperator(plan, selectStmt.getLimit());
            log.debug("Added Limit operator: {}", selectStmt.getLimit());
        }

        return plan;
    }

    /**
     * 构建过滤谓词
     *
     * 简化实现: 只支持简单的比较条件
     * 实际实现: 需要完整的表达式求值器
     */
    private Predicate<Map<String, Object>> buildPredicate(
            com.minidb.parser.ast.Expression expr) {
        // 简化实现: 只处理简单的二元表达式
        if (expr instanceof com.minidb.parser.ast.Expression.BinaryExpression) {
            com.minidb.parser.ast.Expression.BinaryExpression binExpr =
                    (com.minidb.parser.ast.Expression.BinaryExpression) expr;

            String operator = binExpr.getOperator();

            // 获取左侧列名
            if (!(binExpr.getLeft() instanceof com.minidb.parser.ast.Expression.ColumnReference)) {
                throw new UnsupportedOperationException("Complex expressions not supported yet");
            }

            String columnName =
                    ((com.minidb.parser.ast.Expression.ColumnReference) binExpr.getLeft())
                            .getColumnName();

            // 获取右侧值
            if (!(binExpr.getRight() instanceof com.minidb.parser.ast.Expression.LiteralExpression)) {
                throw new UnsupportedOperationException("Complex expressions not supported yet");
            }

            Object value =
                    ((com.minidb.parser.ast.Expression.LiteralExpression) binExpr.getRight())
                            .getValue();

            // 构建谓词
            return row -> {
                Object columnValue = row.get(columnName);
                if (columnValue == null) {
                    return false;
                }

                return evaluateComparison(columnValue, operator, value);
            };
        }

        // 默认: 总是返回 true
        log.warn("Unsupported expression type: {}, returning true", expr.getClass());
        return row -> true;
    }

    /**
     * 执行比较操作
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean evaluateComparison(Object left, String operator, Object right) {
        if (left == null || right == null) {
            return false;
        }

        try {
            // 尝试作为数字比较
            if (left instanceof Number && right instanceof Number) {
                double leftNum = ((Number) left).doubleValue();
                double rightNum = ((Number) right).doubleValue();

                switch (operator) {
                    case "=":
                    case "==":
                        return Math.abs(leftNum - rightNum) < 0.0001;
                    case ">":
                        return leftNum > rightNum;
                    case ">=":
                        return leftNum >= rightNum;
                    case "<":
                        return leftNum < rightNum;
                    case "<=":
                        return leftNum <= rightNum;
                    case "!=":
                    case "<>":
                        return Math.abs(leftNum - rightNum) >= 0.0001;
                    default:
                        return false;
                }
            }

            // 尝试作为 Comparable 比较
            if (left instanceof Comparable && right instanceof Comparable) {
                int cmp = ((Comparable) left).compareTo(right);

                switch (operator) {
                    case "=":
                    case "==":
                        return cmp == 0;
                    case ">":
                        return cmp > 0;
                    case ">=":
                        return cmp >= 0;
                    case "<":
                        return cmp < 0;
                    case "<=":
                        return cmp <= 0;
                    case "!=":
                    case "<>":
                        return cmp != 0;
                    default:
                        return false;
                }
            }

            // 默认: 字符串比较
            return evaluateComparison(left.toString(), operator, right.toString());

        } catch (Exception e) {
            log.warn("Failed to compare {} {} {}: {}", left, operator, right, e.getMessage());
            return false;
        }
    }
}
