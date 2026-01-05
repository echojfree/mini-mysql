package com.minidb.optimizer.stats;

import lombok.Data;

/**
 * 表的统计信息
 *
 * 用于查询优化器估算查询成本，选择最优的执行计划
 *
 * 对应八股文知识点:
 * ✅ MySQL 如何进行查询优化
 * ✅ 统计信息的作用
 * ✅ ANALYZE TABLE 的作用
 * ✅ 为什么统计信息过期会导致查询变慢
 *
 * @author Mini-MySQL
 */
@Data
public class TableStatistics {

    /**
     * 表名
     */
    private String tableName;

    /**
     * 表的总行数
     *
     * 用于:
     * - 估算全表扫描的成本
     * - 计算索引的选择性
     */
    private long rowCount;

    /**
     * 平均行长度（字节）
     *
     * 用于:
     * - 估算 I/O 成本
     * - 计算需要读取的页面数
     */
    private int avgRowLength;

    /**
     * 表的数据文件大小（字节）
     */
    private long dataFileSize;

    /**
     * 表占用的页面数量
     *
     * 页面大小通常为 16KB (InnoDB 默认)
     */
    private long pageCount;

    /**
     * 统计信息最后更新时间
     */
    private long lastUpdateTime;

    /**
     * 计算页面数量
     *
     * @param pageSize 页面大小（字节）
     * @return 页面数量
     */
    public long calculatePageCount(int pageSize) {
        if (dataFileSize == 0 || pageSize == 0) {
            return 0;
        }
        return (dataFileSize + pageSize - 1) / pageSize;
    }

    /**
     * 检查统计信息是否过期
     *
     * 八股文: 统计信息过期会导致优化器选择错误的执行计划
     *
     * @param maxAgeMs 最大年龄（毫秒）
     * @return 是否过期
     */
    public boolean isStale(long maxAgeMs) {
        return System.currentTimeMillis() - lastUpdateTime > maxAgeMs;
    }

    @Override
    public String toString() {
        return String.format("TableStats{table=%s, rows=%d, avgRowLen=%d, pages=%d}",
                tableName, rowCount, avgRowLength, pageCount);
    }
}
