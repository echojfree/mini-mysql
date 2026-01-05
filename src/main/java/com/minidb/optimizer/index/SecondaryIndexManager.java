package com.minidb.optimizer.index;

import com.minidb.storage.index.BPlusTree;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * 二级索引管理器
 *
 * 负责管理表的所有二级索引，包括索引的创建、查询、维护等
 *
 * 核心概念:
 * 1. 聚簇索引: 主键索引，叶子节点存储完整的行数据
 * 2. 二级索引: 非主键索引，叶子节点存储索引列值 + 主键值
 * 3. 回表查询: 先通过二级索引找到主键，再通过聚簇索引查找完整数据
 * 4. 覆盖索引: 查询的列都在索引中，不需要回表
 *
 * 对应八股文知识点:
 * ✅ 聚簇索引 vs 非聚簇索引的区别
 * ✅ 什么是回表查询
 * ✅ 什么是覆盖索引
 * ✅ 二级索引为什么要存主键
 * ✅ 如何减少回表次数
 *
 * @author Mini-MySQL
 */
@Slf4j
public class SecondaryIndexManager {

    /**
     * 表名 -> 索引列表的映射
     */
    private final Map<String, List<IndexMetadata>> tableIndexes;

    /**
     * 表名 -> 聚簇索引的映射
     */
    private final Map<String, IndexMetadata> clusteredIndexes;

    public SecondaryIndexManager() {
        this.tableIndexes = new HashMap<>();
        this.clusteredIndexes = new HashMap<>();
    }

    /**
     * 创建聚簇索引（主键索引）
     *
     * @param tableName 表名
     * @param indexName 索引名
     * @param columnName 主键列名
     * @param order B+ 树阶数
     */
    public void createClusteredIndex(String tableName, String indexName,
                                      String columnName, int order) {
        log.info("Creating clustered index: table={}, index={}, column={}",
                tableName, indexName, columnName);

        IndexMetadata index = new IndexMetadata();
        index.setIndexName(indexName);
        index.setTableName(tableName);
        index.setIndexType(IndexType.CLUSTERED);
        index.setColumnNames(Collections.singletonList(columnName));
        index.setBtree(new BPlusTree<>(order));
        index.setUnique(true);
        index.setCardinality(0);
        index.setSelectivity(1.0);

        clusteredIndexes.put(tableName, index);
    }

    /**
     * 创建二级索引
     *
     * 二级索引的叶子节点存储: 索引列值 -> 主键值
     * 查询时需要回表: 先通过二级索引找主键，再通过聚簇索引找完整数据
     *
     * @param tableName 表名
     * @param indexName 索引名
     * @param columnNames 索引列名列表（支持组合索引）
     * @param unique 是否唯一索引
     * @param order B+ 树阶数
     */
    public void createSecondaryIndex(String tableName, String indexName,
                                      List<String> columnNames, boolean unique, int order) {
        log.info("Creating secondary index: table={}, index={}, columns={}, unique={}",
                tableName, indexName, columnNames, unique);

        IndexMetadata index = new IndexMetadata();
        index.setIndexName(indexName);
        index.setTableName(tableName);
        index.setIndexType(IndexType.SECONDARY);
        index.setColumnNames(new ArrayList<>(columnNames));
        index.setBtree(new BPlusTree<>(order));
        index.setUnique(unique);
        index.setCardinality(0);
        index.setSelectivity(0.0);

        tableIndexes.computeIfAbsent(tableName, k -> new ArrayList<>()).add(index);
    }

    /**
     * 通过二级索引查找主键
     *
     * @param tableName 表名
     * @param indexName 索引名
     * @param indexValue 索引列值
     * @return 主键值，如果未找到返回 null
     */
    @SuppressWarnings("unchecked")
    public <K extends Comparable<K>> K lookupPrimaryKey(String tableName,
                                                          String indexName,
                                                          Comparable<?> indexValue) {
        IndexMetadata index = findIndex(tableName, indexName);
        if (index == null || index.isClustered()) {
            return null;
        }

        // 在二级索引的 B+ 树中查找主键
        Object primaryKey = index.getBtree().search((K) indexValue);
        return (K) primaryKey;
    }

    /**
     * 回表查询: 通过主键在聚簇索引中查找完整的行数据
     *
     * 过程:
     * 1. 在二级索引中查找主键
     * 2. 使用主键在聚簇索引中查找完整数据
     *
     * 八股文: 这就是为什么二级索引查询比聚簇索引慢的原因（需要两次 B+ 树查找）
     *
     * @param tableName 表名
     * @param primaryKey 主键值
     * @return 完整的行数据，如果未找到返回 null
     */
    @SuppressWarnings("unchecked")
    public <K extends Comparable<K>, V> V lookupRow(String tableName, K primaryKey) {
        IndexMetadata clusteredIndex = clusteredIndexes.get(tableName);
        if (clusteredIndex == null) {
            log.warn("No clustered index found for table: {}", tableName);
            return null;
        }

        // 在聚簇索引的 B+ 树中查找完整行数据
        return (V) clusteredIndex.getBtree().search(primaryKey);
    }

    /**
     * 通过二级索引执行完整查询（包含回表）
     *
     * @param tableName 表名
     * @param indexName 索引名
     * @param indexValue 索引列值
     * @return 完整的行数据，如果未找到返回 null
     */
    public <K extends Comparable<K>, V> V queryViaSecondaryIndex(String tableName,
                                                                   String indexName,
                                                                   Comparable<?> indexValue) {
        // 步骤1: 在二级索引中查找主键
        K primaryKey = lookupPrimaryKey(tableName, indexName, indexValue);
        if (primaryKey == null) {
            return null;
        }

        // 步骤2: 回表 - 在聚簇索引中查找完整数据
        return lookupRow(tableName, primaryKey);
    }

    /**
     * 获取表的所有索引
     *
     * @param tableName 表名
     * @return 索引列表
     */
    public List<IndexMetadata> getIndexes(String tableName) {
        List<IndexMetadata> indexes = new ArrayList<>();

        // 添加聚簇索引
        IndexMetadata clustered = clusteredIndexes.get(tableName);
        if (clustered != null) {
            indexes.add(clustered);
        }

        // 添加二级索引
        List<IndexMetadata> secondary = tableIndexes.get(tableName);
        if (secondary != null) {
            indexes.addAll(secondary);
        }

        return indexes;
    }

    /**
     * 查找指定名称的索引
     *
     * @param tableName 表名
     * @param indexName 索引名
     * @return 索引元数据，如果未找到返回 null
     */
    public IndexMetadata findIndex(String tableName, String indexName) {
        // 先检查聚簇索引
        IndexMetadata clustered = clusteredIndexes.get(tableName);
        if (clustered != null && clustered.getIndexName().equals(indexName)) {
            return clustered;
        }

        // 检查二级索引
        List<IndexMetadata> indexes = tableIndexes.get(tableName);
        if (indexes != null) {
            for (IndexMetadata index : indexes) {
                if (index.getIndexName().equals(indexName)) {
                    return index;
                }
            }
        }

        return null;
    }

    /**
     * 更新索引的统计信息
     *
     * @param tableName 表名
     * @param indexName 索引名
     * @param cardinality 基数（不同值的数量）
     * @param totalRows 总行数
     */
    public void updateStatistics(String tableName, String indexName,
                                  long cardinality, long totalRows) {
        IndexMetadata index = findIndex(tableName, indexName);
        if (index == null) {
            return;
        }

        index.setCardinality(cardinality);
        if (totalRows > 0) {
            index.setSelectivity((double) cardinality / totalRows);
        }

        log.debug("Updated index statistics: index={}, cardinality={}, selectivity={}",
                indexName, cardinality, index.getSelectivity());
    }

    /**
     * 删除索引
     *
     * @param tableName 表名
     * @param indexName 索引名
     */
    public void dropIndex(String tableName, String indexName) {
        log.info("Dropping index: table={}, index={}", tableName, indexName);

        // 不能删除聚簇索引
        IndexMetadata clustered = clusteredIndexes.get(tableName);
        if (clustered != null && clustered.getIndexName().equals(indexName)) {
            throw new IllegalArgumentException("Cannot drop clustered index");
        }

        // 删除二级索引
        List<IndexMetadata> indexes = tableIndexes.get(tableName);
        if (indexes != null) {
            indexes.removeIf(idx -> idx.getIndexName().equals(indexName));
        }
    }
}
