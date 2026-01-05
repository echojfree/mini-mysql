package com.minidb.executor.operator;

import com.minidb.executor.Operator;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 投影算子
 *
 * 功能: 选择需要的列（SELECT 子句）
 *
 * 工作原理:
 * 1. 从子算子获取完整的行数据
 * 2. 只保留 SELECT 指定的列
 * 3. 支持列重命名（AS 别名）
 *
 * 对应八股文知识点:
 * ✅ SELECT 的执行过程
 * ✅ 投影操作的作用
 * ✅ SELECT * vs SELECT 指定列的区别
 *
 * @author Mini-MySQL
 */
@Slf4j
public class ProjectOperator implements Operator {

    /**
     * 子算子（数据源）
     */
    private final Operator child;

    /**
     * 要选择的列名列表
     * null 表示 SELECT *
     */
    private final List<String> selectColumns;

    /**
     * 列别名映射
     * 原列名 -> 新列名（AS 别名）
     */
    private final Map<String, String> aliases;

    /**
     * 统计信息
     */
    private int processedRows = 0;

    /**
     * 构造函数 - SELECT *
     */
    public ProjectOperator(Operator child) {
        this(child, null, null);
    }

    /**
     * 构造函数 - SELECT 指定列
     */
    public ProjectOperator(Operator child, List<String> selectColumns) {
        this(child, selectColumns, null);
    }

    /**
     * 构造函数 - SELECT 指定列 + 别名
     */
    public ProjectOperator(Operator child, List<String> selectColumns,
                          Map<String, String> aliases) {
        this.child = child;
        this.selectColumns = selectColumns;
        this.aliases = aliases != null ? aliases : new HashMap<>();
    }

    @Override
    public void open() throws Exception {
        log.debug("Opening Project: columns={}",
                 selectColumns != null ? selectColumns : "*");
        child.open();
        processedRows = 0;
    }

    @Override
    public Map<String, Object> next() throws Exception {
        Map<String, Object> row = child.next();

        if (row == null) {
            log.debug("Project finished, processed {} rows", processedRows);
            return null;
        }

        processedRows++;

        // SELECT * - 返回所有列
        if (selectColumns == null || selectColumns.isEmpty()) {
            return row;
        }

        // SELECT 指定列 - 只保留需要的列
        Map<String, Object> projectedRow = new HashMap<>();
        for (String column : selectColumns) {
            if (row.containsKey(column)) {
                // 检查是否有别名
                String outputColumn = aliases.getOrDefault(column, column);
                projectedRow.put(outputColumn, row.get(column));
            } else {
                log.warn("Column '{}' not found in row: {}", column, row.keySet());
            }
        }

        log.trace("Project row {}: {} -> {}", processedRows, row, projectedRow);
        return projectedRow;
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing Project");
        child.close();
    }

    @Override
    public String getOperatorType() {
        return "Project(" + (selectColumns != null ? selectColumns : "*") + ")";
    }

    public int getProcessedRows() {
        return processedRows;
    }
}
