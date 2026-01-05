package com.minidb.optimizer.cost;

import com.minidb.optimizer.index.IndexMetadata;
import com.minidb.optimizer.stats.TableStatistics;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * 查询成本模型
 *
 * 用于估算不同执行计划的成本，帮助优化器选择最优方案
 *
 * 成本组成:
 * 1. I/O 成本: 读取磁盘页面的成本
 * 2. CPU 成本: 处理数据的成本
 *
 * 对应八股文知识点:
 * ✅ MySQL 如何选择索引
 * ✅ 全表扫描 vs 索引扫描的成本对比
 * ✅ 为什么小表不走索引
 * ✅ 回表的代价
 *
 * @author Mini-MySQL
 */
@Slf4j
@Data
public class CostModel {

    /**
     * I/O 成本权重
     *
     * 读取一个页面的成本
     * MySQL 默认值: 1.0
     */
    private static final double IO_COST_PER_PAGE = 1.0;

    /**
     * CPU 成本权重
     *
     * 处理一行数据的成本
     * MySQL 默认值: 0.2
     */
    private static final double CPU_COST_PER_ROW = 0.2;

    /**
     * 回表成本权重
     *
     * 回表需要额外的随机 I/O，成本较高
     */
    private static final double LOOKUP_COST = 1.5;

    /**
     * 页面大小（字节）
     * InnoDB 默认 16KB
     */
    private static final int PAGE_SIZE = 16 * 1024;

    /**
     * 估算全表扫描的成本
     *
     * 全表扫描成本 = I/O 成本 + CPU 成本
     * - I/O 成本 = 页面数 * IO_COST_PER_PAGE
     * - CPU 成本 = 行数 * CPU_COST_PER_ROW
     *
     * 八股文: 全表扫描是顺序 I/O，比随机 I/O 快
     *
     * @param stats 表统计信息
     * @return 成本估算值
     */
    public double estimateTableScanCost(TableStatistics stats) {
        if (stats == null || stats.getRowCount() == 0) {
            return 0.0;
        }

        // I/O 成本: 读取所有页面
        double ioCost = stats.getPageCount() * IO_COST_PER_PAGE;

        // CPU 成本: 处理所有行
        double cpuCost = stats.getRowCount() * CPU_COST_PER_ROW;

        double totalCost = ioCost + cpuCost;

        log.debug("Table scan cost: table={}, pages={}, rows={}, IO={}, CPU={}, total={}",
                stats.getTableName(), stats.getPageCount(), stats.getRowCount(),
                ioCost, cpuCost, totalCost);

        return totalCost;
    }

    /**
     * 估算索引扫描的成本（不包含回表）
     *
     * 索引扫描成本 = I/O 成本 + CPU 成本
     * - I/O 成本 = log(N) * IO_COST_PER_PAGE (B+ 树高度)
     * - CPU 成本 = 匹配行数 * CPU_COST_PER_ROW
     *
     * 八股文:
     * - 索引扫描是随机 I/O，需要多次磁盘寻址
     * - B+ 树高度通常是 3-4 层
     *
     * @param stats 表统计信息
     * @param index 索引元数据
     * @param filterRate 过滤率（满足条件的行数比例）
     * @return 成本估算值
     */
    public double estimateIndexScanCost(TableStatistics stats,
                                         IndexMetadata index,
                                         double filterRate) {
        if (stats == null || index == null || stats.getRowCount() == 0) {
            return Double.MAX_VALUE;
        }

        // B+ 树高度估算: log_M(N)
        // M 为节点的扇出（假设为 100）
        int fanout = 100;
        int treeHeight = (int) Math.ceil(Math.log(stats.getRowCount()) / Math.log(fanout));

        // I/O 成本: 遍历 B+ 树的高度
        double ioCost = treeHeight * IO_COST_PER_PAGE;

        // CPU 成本: 处理匹配的行
        long matchedRows = (long) (stats.getRowCount() * filterRate);
        double cpuCost = matchedRows * CPU_COST_PER_ROW;

        double totalCost = ioCost + cpuCost;

        log.debug("Index scan cost: index={}, height={}, matchedRows={}, IO={}, CPU={}, total={}",
                index.getIndexName(), treeHeight, matchedRows, ioCost, cpuCost, totalCost);

        return totalCost;
    }

    /**
     * 估算回表的成本
     *
     * 回表过程:
     * 1. 在二级索引中找到主键
     * 2. 使用主键在聚簇索引中查找完整数据
     *
     * 回表成本 = 回表次数 * LOOKUP_COST
     *
     * 八股文:
     * - 回表是随机 I/O，成本很高
     * - 覆盖索引可以避免回表
     * - 回表次数太多时，优化器会放弃使用索引
     *
     * @param matchedRows 匹配的行数（需要回表的次数）
     * @return 成本估算值
     */
    public double estimateLookupCost(long matchedRows) {
        double cost = matchedRows * LOOKUP_COST;

        log.debug("Lookup cost: rows={}, cost={}", matchedRows, cost);

        return cost;
    }

    /**
     * 估算使用二级索引的总成本（包含回表）
     *
     * 总成本 = 索引扫描成本 + 回表成本
     *
     * 八股文:
     * - 当回表次数太多时，全表扫描反而更快
     * - MySQL 优化器会在二者之间权衡
     *
     * @param stats 表统计信息
     * @param index 二级索引元数据
     * @param filterRate 过滤率
     * @param needLookup 是否需要回表（覆盖索引则不需要）
     * @return 总成本
     */
    public double estimateSecondaryIndexCost(TableStatistics stats,
                                              IndexMetadata index,
                                              double filterRate,
                                              boolean needLookup) {
        // 索引扫描成本
        double scanCost = estimateIndexScanCost(stats, index, filterRate);

        // 回表成本
        double lookupCost = 0.0;
        if (needLookup) {
            long matchedRows = (long) (stats.getRowCount() * filterRate);
            lookupCost = estimateLookupCost(matchedRows);
        }

        double totalCost = scanCost + lookupCost;

        log.debug("Secondary index total cost: index={}, scan={}, lookup={}, total={}",
                index.getIndexName(), scanCost, lookupCost, totalCost);

        return totalCost;
    }

    /**
     * 比较两种执行计划的成本
     *
     * @param cost1 执行计划1的成本
     * @param cost2 执行计划2的成本
     * @return 负数表示计划1更优，正数表示计划2更优，0表示相同
     */
    public int compareCost(double cost1, double cost2) {
        double diff = cost1 - cost2;
        if (Math.abs(diff) < 0.001) {
            return 0;
        }
        return diff < 0 ? -1 : 1;
    }

    /**
     * 判断是否应该使用索引
     *
     * 规则:
     * - 如果索引成本 < 全表扫描成本，使用索引
     * - 如果过滤率太高（>30%），不使用索引
     *
     * 八股文:
     * - 当查询返回大量数据时，全表扫描更快
     * - MySQL 优化器会自动选择
     *
     * @param tableScanCost 全表扫描成本
     * @param indexCost 索引扫描成本
     * @param filterRate 过滤率
     * @return 是否应该使用索引
     */
    public boolean shouldUseIndex(double tableScanCost, double indexCost, double filterRate) {
        // 规则1: 过滤率太高，不使用索引
        if (filterRate > 0.3) {
            log.debug("Filter rate too high ({}), skip index", filterRate);
            return false;
        }

        // 规则2: 索引成本更低
        if (indexCost < tableScanCost) {
            log.debug("Index cost ({}) < table scan cost ({}), use index",
                    indexCost, tableScanCost);
            return true;
        }

        log.debug("Table scan cost ({}) <= index cost ({}), skip index",
                tableScanCost, indexCost);
        return false;
    }
}
