package com.minidb.optimizer.index;

import com.minidb.storage.index.BPlusTree;
import lombok.Data;

import java.util.List;

/**
 * 索引元数据
 *
 * 描述一个索引的基本信息，包括索引类型、索引列、底层 B+ 树等
 *
 * 对应八股文知识点:
 * ✅ 什么是索引
 * ✅ 索引的数据结构（B+ 树）
 * ✅ 组合索引（联合索引）
 * ✅ 索引覆盖
 *
 * @author Mini-MySQL
 */
@Data
public class IndexMetadata {

    /**
     * 索引名称
     */
    private String indexName;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 索引类型（聚簇索引或二级索引）
     */
    private IndexType indexType;

    /**
     * 索引列名列表
     *
     * 单列索引: 只有一个列名
     * 组合索引（联合索引）: 包含多个列名
     *
     * 八股文: 组合索引遵循最左前缀原则
     */
    private List<String> columnNames;

    /**
     * 底层 B+ 树结构
     *
     * 聚簇索引: 键是主键，值是完整的行数据
     * 二级索引: 键是索引列值，值是主键
     */
    @SuppressWarnings("rawtypes")
    private BPlusTree btree;

    /**
     * 是否唯一索引
     */
    private boolean unique;

    /**
     * 索引基数（Cardinality）
     *
     * 表示索引列中不同值的数量，用于优化器估算选择性
     *
     * 八股文知识点:
     * - 基数越高，索引选择性越好
     * - 性别字段基数低（2个值），不适合建索引
     * - 用户ID基数高，适合建索引
     */
    private long cardinality;

    /**
     * 索引选择性（Selectivity）
     *
     * 选择性 = 基数 / 总行数
     * 取值范围 [0, 1]，越接近 1 表示选择性越好
     *
     * 八股文: 优化器会根据选择性判断是否使用索引
     */
    private double selectivity;

    /**
     * 检查是否为聚簇索引
     */
    public boolean isClustered() {
        return indexType == IndexType.CLUSTERED;
    }

    /**
     * 检查是否为二级索引
     */
    public boolean isSecondary() {
        return indexType == IndexType.SECONDARY;
    }

    /**
     * 检查索引是否覆盖指定的列
     *
     * 覆盖索引: 查询的列都在索引中，不需要回表
     *
     * @param queryColumns 查询的列名列表
     * @return 是否覆盖
     */
    public boolean covers(List<String> queryColumns) {
        return columnNames.containsAll(queryColumns);
    }

    /**
     * 检查索引是否可以用于查询
     *
     * 最左前缀原则: 组合索引必须从最左边的列开始匹配
     *
     * 例如: 索引 (a, b, c)
     * - WHERE a = 1 可以使用
     * - WHERE a = 1 AND b = 2 可以使用
     * - WHERE a = 1 AND b = 2 AND c = 3 可以使用
     * - WHERE b = 2 不能使用（缺少 a）
     * - WHERE c = 3 不能使用（缺少 a 和 b）
     *
     * @param whereColumns WHERE 子句中的列名列表
     * @return 是否可以使用此索引
     */
    public boolean canUseFor(List<String> whereColumns) {
        if (whereColumns.isEmpty()) {
            return false;
        }

        // 检查最左前缀原则
        for (int i = 0; i < whereColumns.size(); i++) {
            if (i >= columnNames.size()) {
                break;
            }
            if (!columnNames.get(i).equals(whereColumns.get(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        return String.format("Index{name=%s, type=%s, columns=%s, cardinality=%d, selectivity=%.4f}",
                indexName, indexType, columnNames, cardinality, selectivity);
    }
}
