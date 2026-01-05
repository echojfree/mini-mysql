package com.minidb.optimizer.explain;

import com.minidb.optimizer.selector.IndexSelector;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * EXPLAIN 执行计划
 *
 * 用于展示查询的执行计划，帮助开发者理解和优化查询
 *
 * 对应八股文知识点:
 * ✅ 如何使用 EXPLAIN 分析查询
 * ✅ EXPLAIN 各列的含义
 * ✅ type 列的不同值及其性能差异
 * ✅ Extra 列的重要信息
 *
 * @author Mini-MySQL
 */
@Data
public class ExplainPlan {

    /**
     * 查询 ID
     */
    private int id;

    /**
     * 查询类型
     * - SIMPLE: 简单查询（不包含子查询和 UNION）
     * - PRIMARY: 主查询
     * - SUBQUERY: 子查询
     */
    private String selectType;

    /**
     * 表名
     */
    private String table;

    /**
     * 访问类型（从最优到最差）
     *
     * 八股文知识点:
     * - const: 常量查询（主键或唯一索引的等值查询）
     * - eq_ref: 唯一索引查询
     * - ref: 非唯一索引查询
     * - range: 范围查询
     * - index: 索引全扫描
     * - ALL: 全表扫描（最差）
     */
    private String type;

    /**
     * 可能使用的索引列表
     */
    private List<String> possibleKeys;

    /**
     * 实际使用的索引
     */
    private String key;

    /**
     * 使用的索引长度（字节）
     */
    private Integer keyLen;

    /**
     * 索引使用的列数
     */
    private Integer keyColumnsUsed;

    /**
     * 估算的返回行数
     *
     * 八股文: 行数越少越好，说明过滤效果好
     */
    private Long rows;

    /**
     * 过滤百分比
     * 表示经过 WHERE 条件过滤后剩余的行数百分比
     */
    private Double filtered;

    /**
     * 额外信息
     *
     * 重要值:
     * - Using index: 使用覆盖索引（无需回表，性能最好）
     * - Using where: 使用 WHERE 过滤
     * - Using index condition: 使用索引下推
     * - Using filesort: 需要额外排序（性能较差）
     * - Using temporary: 使用临时表（性能较差）
     */
    private List<String> extra;

    /**
     * 估算的查询成本
     */
    private Double cost;

    public ExplainPlan() {
        this.possibleKeys = new ArrayList<>();
        this.extra = new ArrayList<>();
    }

    /**
     * 添加额外信息
     */
    public void addExtra(String info) {
        if (!extra.contains(info)) {
            extra.add(info);
        }
    }

    /**
     * 添加可能的索引
     */
    public void addPossibleKey(String indexName) {
        if (!possibleKeys.contains(indexName)) {
            possibleKeys.add(indexName);
        }
    }

    /**
     * 格式化输出
     *
     * 模拟 MySQL EXPLAIN 的输出格式
     */
    public String format() {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("id: %d\n", id));
        sb.append(String.format("select_type: %s\n", selectType));
        sb.append(String.format("table: %s\n", table));
        sb.append(String.format("type: %s\n", type));

        if (!possibleKeys.isEmpty()) {
            sb.append(String.format("possible_keys: %s\n", String.join(", ", possibleKeys)));
        }

        if (key != null) {
            sb.append(String.format("key: %s\n", key));
        }

        if (keyLen != null) {
            sb.append(String.format("key_len: %d\n", keyLen));
        }

        if (rows != null) {
            sb.append(String.format("rows: %d\n", rows));
        }

        if (filtered != null) {
            sb.append(String.format("filtered: %.2f%%\n", filtered));
        }

        if (!extra.isEmpty()) {
            sb.append(String.format("Extra: %s\n", String.join("; ", extra)));
        }

        if (cost != null) {
            sb.append(String.format("cost: %.2f\n", cost));
        }

        return sb.toString();
    }

    /**
     * 表格格式输出
     */
    public String formatTable() {
        return String.format("| %2d | %-10s | %-15s | %-8s | %-20s | %-15s | %6d | %6.2f%% | %-30s |",
                id,
                selectType,
                table,
                type,
                possibleKeys.isEmpty() ? "NULL" : String.join(",", possibleKeys),
                key != null ? key : "NULL",
                rows != null ? rows : 0,
                filtered != null ? filtered : 100.0,
                extra.isEmpty() ? "" : String.join("; ", extra));
    }

    /**
     * 表格表头
     */
    public static String tableHeader() {
        return String.format("| %2s | %-10s | %-15s | %-8s | %-20s | %-15s | %6s | %8s | %-30s |",
                "id", "select_type", "table", "type", "possible_keys", "key", "rows", "filtered", "Extra");
    }

    /**
     * 表格分隔线
     */
    public static String tableSeparator() {
        return "+----+------------+-----------------+----------+----------------------+-----------------+--------+----------+--------------------------------+";
    }

    @Override
    public String toString() {
        return format();
    }
}
