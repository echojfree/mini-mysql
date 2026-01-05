package com.minidb.executor.operator;

import com.minidb.executor.Operator;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 全表扫描算子
 *
 * 功能: 顺序扫描表中的所有行
 *
 * 特点:
 * - 不使用索引
 * - 按照物理存储顺序读取
 * - 适合小表或没有合适索引的查询
 *
 * 对应八股文知识点:
 * ✅ 什么是全表扫描
 * ✅ 全表扫描 vs 索引扫描
 * ✅ 什么时候会用到全表扫描
 * ✅ 如何避免全表扫描
 *
 * @author Mini-MySQL
 */
@Slf4j
public class TableScanOperator implements Operator {

    /**
     * 表名
     */
    private final String tableName;

    /**
     * 表中的所有数据
     * 简化实现: 直接从内存中读取
     * 实际 MySQL: 从磁盘页面中读取
     */
    private final List<Map<String, Object>> tableData;

    /**
     * 当前迭代器
     */
    private Iterator<Map<String, Object>> iterator;

    /**
     * 已扫描的行数
     */
    private int scannedRows;

    public TableScanOperator(String tableName, List<Map<String, Object>> tableData) {
        this.tableName = tableName;
        this.tableData = tableData;
        this.scannedRows = 0;
    }

    @Override
    public void open() throws Exception {
        log.debug("Opening TableScan on table: {}", tableName);
        iterator = tableData.iterator();
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
            log.trace("TableScan returned row {}: {}", scannedRows, row);
            return row;
        }

        log.debug("TableScan finished, scanned {} rows", scannedRows);
        return null;
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing TableScan on table: {}, total rows scanned: {}",
                 tableName, scannedRows);
        iterator = null;
    }

    @Override
    public String getOperatorType() {
        return "TableScan(" + tableName + ")";
    }

    /**
     * 获取已扫描的行数
     */
    public int getScannedRows() {
        return scannedRows;
    }
}
