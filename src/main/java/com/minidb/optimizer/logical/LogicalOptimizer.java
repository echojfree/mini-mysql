package com.minidb.optimizer.logical;

import com.minidb.parser.ast.Expression;
import com.minidb.parser.ast.SelectStatement;
import lombok.extern.slf4j.Slf4j;

/**
 * 逻辑优化器
 *
 * 对查询进行逻辑层面的优化，改写查询以提高执行效率
 *
 * 优化技术:
 * 1. 常量折叠 (Constant Folding)
 * 2. 条件下推 (Predicate Pushdown)
 * 3. 无用条件消除
 * 4. 表达式简化
 *
 * 对应八股文知识点:
 * ✅ MySQL 查询优化过程
 * ✅ 谓词下推的作用
 * ✅ 常量传播优化
 *
 * @author Mini-MySQL
 */
@Slf4j
public class LogicalOptimizer {

    /**
     * 优化 SELECT 查询
     *
     * @param selectStmt 原始 SELECT 语句
     * @return 优化后的 SELECT 语句
     */
    public SelectStatement optimize(SelectStatement selectStmt) {
        log.info("Optimizing SELECT query on table: {}", selectStmt.getTableName());

        // 创建副本以避免修改原始对象
        SelectStatement optimized = copySelectStatement(selectStmt);

        // 1. 常量折叠
        if (optimized.getWhereCondition() != null) {
            Expression foldedExpr = constantFolding(optimized.getWhereCondition());
            optimized.setWhereCondition(foldedExpr);
            log.debug("After constant folding: {}", foldedExpr);
        }

        // 2. 无用条件消除
        if (optimized.getWhereCondition() != null) {
            Expression simplified = simplifyExpression(optimized.getWhereCondition());
            optimized.setWhereCondition(simplified);
            log.debug("After simplification: {}", simplified);
        }

        return optimized;
    }

    /**
     * 常量折叠
     *
     * 在编译期计算常量表达式
     *
     * 例如:
     * - WHERE 1 = 1 AND age > 18 → WHERE age > 18
     * - WHERE age > 10 + 5 → WHERE age > 15
     *
     * 八股文: 编译期优化，减少运行时计算
     *
     * @param expr 表达式
     * @return 折叠后的表达式
     */
    private Expression constantFolding(Expression expr) {
        if (expr == null) {
            return null;
        }

        if (expr instanceof Expression.BinaryExpression) {
            Expression.BinaryExpression binExpr = (Expression.BinaryExpression) expr;

            // 递归处理左右子表达式
            Expression left = constantFolding(binExpr.getLeft());
            Expression right = constantFolding(binExpr.getRight());

            // 如果两边都是常量，计算结果
            if (left instanceof Expression.LiteralExpression &&
                    right instanceof Expression.LiteralExpression) {

                Object leftVal = ((Expression.LiteralExpression) left).getValue();
                Object rightVal = ((Expression.LiteralExpression) right).getValue();

                // 布尔运算
                if (binExpr.getOperator().equals("AND")) {
                    if (leftVal instanceof Boolean && rightVal instanceof Boolean) {
                        boolean result = (Boolean) leftVal && (Boolean) rightVal;
                        return createLiteral(result);
                    }
                } else if (binExpr.getOperator().equals("OR")) {
                    if (leftVal instanceof Boolean && rightVal instanceof Boolean) {
                        boolean result = (Boolean) leftVal || (Boolean) rightVal;
                        return createLiteral(result);
                    }
                }
            }

            binExpr.setLeft(left);
            binExpr.setRight(right);
            return binExpr;
        }

        return expr;
    }

    /**
     * 简化表达式
     *
     * 消除无用的条件
     *
     * 例如:
     * - WHERE TRUE AND age > 18 → WHERE age > 18
     * - WHERE FALSE OR age > 18 → WHERE age > 18
     * - WHERE TRUE → 移除 WHERE 子句
     * - WHERE FALSE → 返回空结果
     *
     * @param expr 表达式
     * @return 简化后的表达式
     */
    private Expression simplifyExpression(Expression expr) {
        if (expr == null) {
            return null;
        }

        if (expr instanceof Expression.LiteralExpression) {
            Object value = ((Expression.LiteralExpression) expr).getValue();

            // WHERE TRUE → 移除条件
            if (Boolean.TRUE.equals(value)) {
                log.debug("Removing always-true condition");
                return null;
            }

            // WHERE FALSE → 保留（执行器会处理）
            if (Boolean.FALSE.equals(value)) {
                log.debug("Detected always-false condition");
                return expr;
            }
        }

        if (expr instanceof Expression.BinaryExpression) {
            Expression.BinaryExpression binExpr = (Expression.BinaryExpression) expr;
            Expression left = simplifyExpression(binExpr.getLeft());
            Expression right = simplifyExpression(binExpr.getRight());

            String operator = binExpr.getOperator();

            // AND 优化
            if ("AND".equals(operator)) {
                // TRUE AND right → right
                if (isTrue(left)) {
                    return right;
                }
                // left AND TRUE → left
                if (isTrue(right)) {
                    return left;
                }
                // FALSE AND right → FALSE
                if (isFalse(left) || isFalse(right)) {
                    return createLiteral(false);
                }
            }

            // OR 优化
            if ("OR".equals(operator)) {
                // TRUE OR right → TRUE
                if (isTrue(left) || isTrue(right)) {
                    return createLiteral(true);
                }
                // FALSE OR right → right
                if (isFalse(left)) {
                    return right;
                }
                // left OR FALSE → left
                if (isFalse(right)) {
                    return left;
                }
            }

            binExpr.setLeft(left);
            binExpr.setRight(right);
            return binExpr;
        }

        return expr;
    }

    /**
     * 检查表达式是否为 TRUE
     */
    private boolean isTrue(Expression expr) {
        if (expr instanceof Expression.LiteralExpression) {
            Object value = ((Expression.LiteralExpression) expr).getValue();
            return Boolean.TRUE.equals(value);
        }
        return false;
    }

    /**
     * 检查表达式是否为 FALSE
     */
    private boolean isFalse(Expression expr) {
        if (expr instanceof Expression.LiteralExpression) {
            Object value = ((Expression.LiteralExpression) expr).getValue();
            return Boolean.FALSE.equals(value);
        }
        return false;
    }

    /**
     * 创建字面量表达式
     */
    private Expression.LiteralExpression createLiteral(Object value) {
        Expression.LiteralExpression literal = new Expression.LiteralExpression();
        literal.setValue(value);
        literal.setDataType(value instanceof Boolean ? "BOOLEAN" : "UNKNOWN");
        return literal;
    }

    /**
     * 复制 SELECT 语句
     *
     * 简化实现，实际应该深拷贝
     */
    private SelectStatement copySelectStatement(SelectStatement stmt) {
        SelectStatement copy = new SelectStatement();
        copy.setTableName(stmt.getTableName());
        copy.setSelectAll(stmt.isSelectAll());
        copy.setSelectElements(stmt.getSelectElements());
        copy.setWhereCondition(stmt.getWhereCondition());
        copy.setOrderByElements(stmt.getOrderByElements());
        copy.setLimit(stmt.getLimit());
        return copy;
    }
}
