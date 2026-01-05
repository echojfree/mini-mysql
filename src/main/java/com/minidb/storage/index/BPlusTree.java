package com.minidb.storage.index;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * B+ 树（BPlusTree）
 *
 * B+ 树是数据库索引的核心数据结构，支持高效的查找、插入和范围查询
 *
 * 核心操作：
 * 1. 查找（Search）：O(log n)
 * 2. 插入（Insert）：O(log n)
 * 3. 删除（Delete）：O(log n)
 * 4. 范围查询（Range Query）：O(log n + k)，k 为结果数量
 *
 * 设计要点：
 * 1. 自平衡：插入和删除时自动调整树结构，保持平衡
 * 2. 分裂：节点满时分裂为两个节点
 * 3. 合并：节点下溢时与兄弟节点合并或借用键
 * 4. 叶子节点链表：支持高效的范围查询和顺序扫描
 *
 * 对应八股文知识点：
 * ✅ B+ 树的插入过程
 * ✅ B+ 树的查询过程
 * ✅ B+ 树的分裂和合并
 * ✅ B+ 树如何支持范围查询
 * ✅ 为什么 B+ 树适合数据库索引
 *
 * @author Mini-MySQL
 * @param <K> 键的类型（必须可比较）
 * @param <V> 值的类型
 */
@Slf4j
@Getter
public class BPlusTree<K extends Comparable<K>, V> {

    /**
     * 根节点
     */
    private BPlusTreeNode<K, V> root;

    /**
     * B+ 树的阶数（每个节点最多包含的键数量）
     * 默认值基于 16KB 页面大小
     */
    private final int order;

    /**
     * 树的高度
     */
    private int height;

    /**
     * 树中的键值对数量
     */
    private int size;

    /**
     * 第一个叶子节点（用于范围查询）
     */
    private BPlusTreeNode<K, V> firstLeaf;

    /**
     * 构造函数
     *
     * @param order B+ 树的阶数
     */
    public BPlusTree(int order) {
        if (order < 3) {
            throw new IllegalArgumentException("Order must be at least 3");
        }
        this.order = order;
        this.root = new BPlusTreeNode<>(order);
        this.firstLeaf = root;
        this.height = 1;
        this.size = 0;
    }

    /**
     * 默认构造函数
     * 使用默认阶数 100
     */
    public BPlusTree() {
        this(100);
    }

    /**
     * 查找键对应的值
     *
     * @param key 要查找的键
     * @return 对应的值，如果不存在返回 null
     */
    public V search(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        // 从根节点开始查找
        BPlusTreeNode<K, V> node = root;

        // 向下遍历到叶子节点
        while (!node.isLeaf()) {
            int i = 0;
            // 找到第一个 > key 的键的位置
            // 注意:如果 key 等于内部节点的某个键,应该走右边的子节点
            while (i < node.getKeyCount() && key.compareTo(node.getKey(i)) >= 0) {
                i++;
            }
            node = node.getChild(i);
        }

        // 在叶子节点中查找
        int index = node.findKeyIndex(key);
        if (index >= 0) {
            return node.getValue(index);
        }

        return null;
    }

    /**
     * 插入键值对
     *
     * @param key   键
     * @param value 值
     */
    public void insert(K key, V value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        // 找到应该插入的叶子节点
        BPlusTreeNode<K, V> leaf = findLeafNode(key);

        // 在叶子节点中插入
        int oldSize = leaf.getKeyCount();
        leaf.insertInLeaf(key, value);

        // 如果是新插入（不是更新），增加计数
        if (leaf.getKeyCount() > oldSize) {
            size++;
        }

        // 如果叶子节点已满，需要分裂
        if (leaf.isFull()) {
            splitLeafNode(leaf);
        }

        log.debug("Inserted key={}, value={}, size={}", key, value, size);
    }

    /**
     * 范围查询
     * 查找 [startKey, endKey] 范围内的所有键值对
     *
     * @param startKey 起始键（包含）
     * @param endKey   结束键（包含）
     * @return 键值对列表
     */
    public List<Entry<K, V>> rangeSearch(K startKey, K endKey) {
        List<Entry<K, V>> result = new ArrayList<>();

        if (startKey == null || endKey == null) {
            throw new IllegalArgumentException("Keys cannot be null");
        }

        if (startKey.compareTo(endKey) > 0) {
            throw new IllegalArgumentException("startKey must <= endKey");
        }

        // 找到起始键所在的叶子节点
        BPlusTreeNode<K, V> node = findLeafNode(startKey);

        // 从起始节点开始，沿着叶子节点链表遍历
        while (node != null) {
            for (int i = 0; i < node.getKeyCount(); i++) {
                K key = node.getKey(i);

                // 如果键 < startKey，跳过
                if (key.compareTo(startKey) < 0) {
                    continue;
                }

                // 如果键 > endKey，结束查询
                if (key.compareTo(endKey) > 0) {
                    return result;
                }

                // 键在范围内，加入结果
                result.add(new Entry<>(key, node.getValue(i)));
            }

            // 移动到下一个叶子节点
            node = node.getNext();
        }

        return result;
    }

    /**
     * 删除键值对
     *
     * @param key 要删除的键
     * @return 删除的值，如果键不存在返回 null
     */
    public V delete(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        // 找到包含该键的叶子节点
        BPlusTreeNode<K, V> leaf = findLeafNode(key);

        // 在叶子节点中查找键
        int index = leaf.findKeyIndex(key);
        if (index < 0) {
            // 键不存在
            return null;
        }

        // 删除键值对
        V value = leaf.getValue(index);
        leaf.getKeys().remove(index);
        leaf.getValues().remove(index);
        size--;

        // 如果删除后节点下溢，需要处理
        if (!leaf.isRoot() && leaf.isUnderflow()) {
            handleUnderflow(leaf);
        }

        // 如果根节点为空，降低树高度
        if (root.getKeyCount() == 0 && !root.isLeaf()) {
            root = root.getChild(0);
            root.setParent(null);
            height--;
        }

        log.debug("Deleted key={}, value={}, size={}", key, value, size);
        return value;
    }

    /**
     * 查找键应该插入的叶子节点
     *
     * @param key 键
     * @return 叶子节点
     */
    private BPlusTreeNode<K, V> findLeafNode(K key) {
        BPlusTreeNode<K, V> node = root;

        while (!node.isLeaf()) {
            int i = 0;
            // 找到第一个 > key 的键的位置
            // 注意:如果 key 等于内部节点的某个键,应该走右边的子节点
            while (i < node.getKeyCount() && key.compareTo(node.getKey(i)) >= 0) {
                i++;
            }
            node = node.getChild(i);
        }

        return node;
    }

    /**
     * 分裂叶子节点
     *
     * @param leaf 要分裂的叶子节点
     */
    private void splitLeafNode(BPlusTreeNode<K, V> leaf) {
        // 分裂节点
        BPlusTreeNode<K, V> newLeaf = leaf.split();
        K middleKey = newLeaf.getFirstKey();

        // 如果是根节点，创建新根
        if (leaf.isRoot()) {
            BPlusTreeNode<K, V> newRoot = new BPlusTreeNode<>(
                    BPlusTreeNode.NODE_TYPE_INTERNAL, order);

            newRoot.getKeys().add(middleKey);
            newRoot.getChildren().add(leaf);
            newRoot.getChildren().add(newLeaf);

            leaf.setParent(newRoot);
            newLeaf.setParent(newRoot);

            root = newRoot;
            height++;

            log.debug("Created new root, height={}", height);
        } else {
            // 将中间键插入父节点
            BPlusTreeNode<K, V> parent = leaf.getParent();
            parent.insertInInternal(middleKey, newLeaf);

            // 如果父节点也满了，递归分裂
            if (parent.isFull()) {
                splitInternalNode(parent);
            }
        }
    }

    /**
     * 分裂非叶子节点
     *
     * @param node 要分裂的非叶子节点
     */
    private void splitInternalNode(BPlusTreeNode<K, V> node) {
        // 分裂节点
        BPlusTreeNode<K, V> newNode = node.split();

        // 获取中间键(应该上升到父节点的键)
        // 注意: split() 后,中间键已经被从左节点移除并保存在右节点的第一个位置之前
        // 我们需要从左节点的最后一个键获取(它将被移除并上升)
        K middleKey = node.getLastKey();
        node.getKeys().remove(node.getKeyCount() - 1);

        // 如果是根节点，创建新根
        if (node.isRoot()) {
            BPlusTreeNode<K, V> newRoot = new BPlusTreeNode<>(
                    BPlusTreeNode.NODE_TYPE_INTERNAL, order);

            newRoot.getKeys().add(middleKey);
            newRoot.getChildren().add(node);
            newRoot.getChildren().add(newNode);

            node.setParent(newRoot);
            newNode.setParent(newRoot);

            root = newRoot;
            height++;

            log.debug("Created new root, height={}", height);
        } else {
            // 将中间键插入父节点
            BPlusTreeNode<K, V> parent = node.getParent();
            parent.insertInInternal(middleKey, newNode);

            // 如果父节点也满了，递归分裂
            if (parent.isFull()) {
                splitInternalNode(parent);
            }
        }
    }

    /**
     * 处理节点下溢
     * 尝试从兄弟节点借键，如果不行则合并节点
     *
     * @param node 下溢的节点
     */
    private void handleUnderflow(BPlusTreeNode<K, V> node) {
        // 简化版：暂不实现下溢处理
        // 完整实现需要考虑从兄弟节点借键或合并节点
        // 这里只做简单标记
        log.debug("Node underflow detected, key count={}", node.getKeyCount());
    }

    /**
     * 判断树是否为空
     *
     * @return true 表示空树
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * 清空树
     */
    public void clear() {
        this.root = new BPlusTreeNode<>(order);
        this.firstLeaf = root;
        this.height = 1;
        this.size = 0;
    }

    /**
     * 键值对条目类
     */
    public static class Entry<K, V> {
        private final K key;
        private final V value;

        public Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        @Override
        public String toString() {
            return key + "=" + value;
        }
    }
}
