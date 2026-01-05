package com.minidb.executor.operator;

import com.minidb.executor.Operator;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * 限制算子
 *
 * 功能: LIMIT 限制返回的行数
 *
 * 工作原理:
 * 1. 从子算子获取数据
 * 2. 返回前 N 行
 * 3. 达到限制后停止
 *
 * 对应八股文知识点:
 * ✅ LIMIT 的执行过程
 * ✅ LIMIT 优化（提前终止）
 * ✅ 分页查询的实现
 *
 * @author Mini-MySQL
 */
@Slf4j
public class LimitOperator implements Operator {

    /**
     * 子算子（数据源）
     */
    private final Operator child;

    /**
     * 限制的行数
     */
    private final int limit;

    /**
     * 已返回的行数
     */
    private int returnedRows = 0;

    public LimitOperator(Operator child, int limit) {
        this.child = child;
        this.limit = limit;
    }

    @Override
    public void open() throws Exception {
        log.debug("Opening Limit: {}", limit);
        child.open();
        returnedRows = 0;
    }

    @Override
    public Map<String, Object> next() throws Exception {
        // 已达到限制，停止返回数据
        if (returnedRows >= limit) {
            log.debug("Limit reached: {} rows", returnedRows);
            return null;
        }

        Map<String, Object> row = child.next();

        if (row != null) {
            returnedRows++;
            log.trace("Limit returned row {}/{}", returnedRows, limit);
        }

        return row;
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing Limit, returned {} rows", returnedRows);
        child.close();
    }

    @Override
    public String getOperatorType() {
        return "Limit(" + limit + ")";
    }

    public int getReturnedRows() {
        return returnedRows;
    }
}
