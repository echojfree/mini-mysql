package com.minidb.executor.operator;

import com.minidb.executor.Operator;
import com.minidb.parser.ast.Expression;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.function.Predicate;

/**
 * 过滤算子
 *
 * 功能: 根据 WHERE 条件过滤数据
 *
 * 工作原理:
 * 1. 从子算子获取数据
 * 2. 对每行数据应用过滤条件
 * 3. 只返回满足条件的行
 *
 * 对应八股文知识点:
 * ✅ WHERE 条件的执行过程
 * ✅ 过滤算子的作用
 * ✅ 谓词下推优化
 *
 * @author Mini-MySQL
 */
@Slf4j
public class FilterOperator implements Operator {

    /**
     * 子算子（数据源）
     */
    private final Operator child;

    /**
     * 过滤条件
     * 简化实现: 使用 Java 的 Predicate
     * 实际实现: 需要解释执行 Expression AST
     */
    private final Predicate<Map<String, Object>> predicate;

    /**
     * 过滤描述（用于日志和 EXPLAIN）
     */
    private final String filterDescription;

    /**
     * 统计信息
     */
    private int inputRows = 0;
    private int outputRows = 0;

    public FilterOperator(Operator child, Predicate<Map<String, Object>> predicate,
                          String filterDescription) {
        this.child = child;
        this.predicate = predicate;
        this.filterDescription = filterDescription;
    }

    @Override
    public void open() throws Exception {
        log.debug("Opening Filter: {}", filterDescription);
        child.open();
        inputRows = 0;
        outputRows = 0;
    }

    @Override
    public Map<String, Object> next() throws Exception {
        // 循环从子算子获取数据，直到找到满足条件的行
        while (true) {
            Map<String, Object> row = child.next();

            if (row == null) {
                // 没有更多数据
                log.debug("Filter finished: input={}, output={}, selectivity={:.2f}%",
                         inputRows, outputRows,
                         inputRows > 0 ? (outputRows * 100.0 / inputRows) : 0);
                return null;
            }

            inputRows++;

            // 应用过滤条件
            if (predicate.test(row)) {
                outputRows++;
                log.trace("Filter passed row {}: {}", outputRows, row);
                return row;
            } else {
                log.trace("Filter rejected row: {}", row);
            }
        }
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing Filter: {}", filterDescription);
        child.close();
    }

    @Override
    public String getOperatorType() {
        return "Filter(" + filterDescription + ")";
    }

    /**
     * 获取过滤选择性
     * 选择性 = 输出行数 / 输入行数
     */
    public double getSelectivity() {
        return inputRows > 0 ? (double) outputRows / inputRows : 0;
    }

    public int getInputRows() {
        return inputRows;
    }

    public int getOutputRows() {
        return outputRows;
    }
}
