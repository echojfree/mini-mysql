package com.minidb.storage.index;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * B+ 树节点（BPlusTreeNode）
 *
 * B+ 树是一种多路平衡查找树，是数据库索引的核心数据结构
 *
 * B+ 树的特点：
 * 1. 所有数据都存储在叶子节点
 * 2. 非叶子节点只存储键值和指针（用于导航）
 * 3. 叶子节点之间通过指针连接（支持范围查询）
 * 4. 所有叶子节点在同一层（树高度平衡）
 *
 * 为什么用 B+ 树而不是 B 树？
 * 1. B+ 树的非叶子节点不存储数据，可以存储更多的键值，减少树高度
 * 2. B+ 树的叶子节点有链表连接，支持高效的范围查询
 * 3. B+ 树的查询更稳定，所有查询都要到达叶子节点（路径长度相同）
 *
 * 为什么用 B+ 树而不是红黑树或 AVL 树？
 * 1. B+ 树是多路树，树高度更低，磁盘 I/O 次数更少
 * 2. B+ 树的节点大小与磁盘页大小匹配（16KB），利用磁盘预读
 * 3. B+ 树对磁盘友好，红黑树/AVL 树更适合内存操作
 *
 * 为什么 3 层 B+ 树能存储 2000 万数据？
 * 假设：
 * - 每个非叶子节点可以存储 1000 个键值（16KB / 16B ≈ 1000）
 * - 第 1 层（根节点）：1 个节点
 * - 第 2 层：1000 个节点
 * - 第 3 层（叶子节点）：1000 * 1000 = 100 万个节点
 * - 每个叶子节点存储 20 条数据（16KB / 800B ≈ 20）
 * - 总数据量：100 万 * 20 = 2000 万条
 *
 * @author Mini-MySQL
 * @param <K> 键的类型（必须可比较）
 * @param <V> 值的类型
 */
@Data
public class BPlusTreeNode<K extends Comparable<K>, V> {

    /**
     * 节点类型：叶子节点
     */
    public static final int NODE_TYPE_LEAF = 1;

    /**
     * 节点类型：非叶子节点（内部节点）
     */
    public static final int NODE_TYPE_INTERNAL = 2;

    /**
     * 节点类型
     */
    private int nodeType;

    /**
     * 父节点
     */
    private BPlusTreeNode<K, V> parent;

    /**
     * 键列表
     * - 对于叶子节点：存储实际的键
     * - 对于非叶子节点：存储用于导航的键（子节点的最小键）
     */
    private List<K> keys;

    /**
     * 值列表（仅叶子节点使用）
     * 存储键对应的数据值
     */
    private List<V> values;

    /**
     * 子节点列表（仅非叶子节点使用）
     * children[i] 指向的子树中的所有键都 >= keys[i]
     */
    private List<BPlusTreeNode<K, V>> children;

    /**
     * 下一个叶子节点（仅叶子节点使用）
     * 用于构建叶子节点的链表，支持范围查询
     */
    private BPlusTreeNode<K, V> next;

    /**
     * 前一个叶子节点（仅叶子节点使用）
     * 用于构建双向链表，支持逆序范围查询
     */
    private BPlusTreeNode<K, V> previous;

    /**
     * 节点的最大键数量（阶数）
     * 当键数量超过此值时，需要分裂
     */
    private int maxKeys;

    /**
     * 构造函数：创建叶子节点
     *
     * @param maxKeys 最大键数量
     */
    public BPlusTreeNode(int maxKeys) {
        this(NODE_TYPE_LEAF, maxKeys);
    }

    /**
     * 构造函数：创建指定类型的节点
     *
     * @param nodeType 节点类型
     * @param maxKeys  最大键数量
     */
    public BPlusTreeNode(int nodeType, int maxKeys) {
        this.nodeType = nodeType;
        this.maxKeys = maxKeys;
        this.keys = new ArrayList<>();

        if (isLeaf()) {
            // 叶子节点需要值列表
            this.values = new ArrayList<>();
        } else {
            // 非叶子节点需要子节点列表
            this.children = new ArrayList<>();
        }
    }

    /**
     * 判断是否为叶子节点
     *
     * @return true 表示叶子节点
     */
    public boolean isLeaf() {
        return nodeType == NODE_TYPE_LEAF;
    }

    /**
     * 判断节点是否已满
     * 已满的节点需要分裂
     *
     * @return true 表示已满
     */
    public boolean isFull() {
        return keys.size() >= maxKeys;
    }

    /**
     * 判断节点是否下溢
     * 下溢的节点需要合并或借用兄弟节点的键
     *
     * @return true 表示下溢
     */
    public boolean isUnderflow() {
        // 最小键数量 = maxKeys / 2
        int minKeys = maxKeys / 2;
        return keys.size() < minKeys;
    }

    /**
     * 获取键的数量
     *
     * @return 键的数量
     */
    public int getKeyCount() {
        return keys.size();
    }

    /**
     * 获取指定索引的键
     *
     * @param index 索引
     * @return 键
     */
    public K getKey(int index) {
        if (index < 0 || index >= keys.size()) {
            throw new IndexOutOfBoundsException("Invalid key index: " + index);
        }
        return keys.get(index);
    }

    /**
     * 获取指定索引的值（仅叶子节点）
     *
     * @param index 索引
     * @return 值
     */
    public V getValue(int index) {
        if (!isLeaf()) {
            throw new UnsupportedOperationException("Internal node does not have values");
        }
        if (index < 0 || index >= values.size()) {
            throw new IndexOutOfBoundsException("Invalid value index: " + index);
        }
        return values.get(index);
    }

    /**
     * 获取指定索引的子节点（仅非叶子节点）
     *
     * @param index 索引
     * @return 子节点
     */
    public BPlusTreeNode<K, V> getChild(int index) {
        if (isLeaf()) {
            throw new UnsupportedOperationException("Leaf node does not have children");
        }
        if (index < 0 || index >= children.size()) {
            throw new IndexOutOfBoundsException("Invalid child index: " + index);
        }
        return children.get(index);
    }

    /**
     * 在节点中查找键的位置
     * 使用二分查找，时间复杂度 O(log n)
     *
     * @param key 要查找的键
     * @return 键的索引，如果不存在则返回应该插入的位置（负数）
     */
    public int findKeyIndex(K key) {
        int left = 0;
        int right = keys.size() - 1;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            int cmp = key.compareTo(keys.get(mid));

            if (cmp == 0) {
                return mid; // 找到了
            } else if (cmp < 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        // 没找到，返回应该插入的位置（负数表示）
        return -(left + 1);
    }

    /**
     * 在叶子节点中插入键值对
     *
     * @param key   键
     * @param value 值
     * @return 插入位置的索引
     */
    public int insertInLeaf(K key, V value) {
        if (!isLeaf()) {
            throw new UnsupportedOperationException("Can only insert in leaf node");
        }

        int index = findKeyIndex(key);

        if (index >= 0) {
            // 键已存在，更新值
            values.set(index, value);
            return index;
        } else {
            // 键不存在，插入新键值对
            int insertPos = -(index + 1);
            keys.add(insertPos, key);
            values.add(insertPos, value);
            return insertPos;
        }
    }

    /**
     * 在非叶子节点中插入键和子节点
     *
     * @param key   键
     * @param child 子节点
     */
    public void insertInInternal(K key, BPlusTreeNode<K, V> child) {
        if (isLeaf()) {
            throw new UnsupportedOperationException("Can only insert in internal node");
        }

        int index = findKeyIndex(key);
        int insertPos = index >= 0 ? index : -(index + 1);

        keys.add(insertPos, key);
        children.add(insertPos + 1, child);
        child.setParent(this);
    }

    /**
     * 分裂节点
     * 当节点已满时，将节点分裂为两个节点
     *
     * @return 分裂后的新节点（右节点）
     */
    public BPlusTreeNode<K, V> split() {
        int mid = keys.size() / 2;
        BPlusTreeNode<K, V> newNode = new BPlusTreeNode<>(nodeType, maxKeys);

        if (isLeaf()) {
            // 叶子节点分裂
            // 右节点包含 [mid, end) 的键值对
            newNode.keys.addAll(keys.subList(mid, keys.size()));
            newNode.values.addAll(values.subList(mid, values.size()));

            // 左节点保留 [0, mid) 的键值对
            keys.subList(mid, keys.size()).clear();
            values.subList(mid, values.size()).clear();

            // 维护叶子节点链表
            newNode.next = this.next;
            newNode.previous = this;
            if (this.next != null) {
                this.next.previous = newNode;
            }
            this.next = newNode;

        } else {
            // 非叶子节点分裂
            // 中间键 keys[mid] 会上升到父节点
            // 左节点保留 [0, mid) 的键和 [0, mid+1) 的子节点
            // 右节点包含 [mid+1, end) 的键和 [mid+1, end) 的子节点

            newNode.keys.addAll(keys.subList(mid + 1, keys.size()));
            newNode.children.addAll(children.subList(mid + 1, children.size()));

            // 更新子节点的父指针
            for (BPlusTreeNode<K, V> child : newNode.children) {
                child.setParent(newNode);
            }

            // 左节点保留 [0, mid) 的键
            // 注意: keys[mid] 将在 splitInternalNode 中被取出并上升到父节点
            keys.subList(mid + 1, keys.size()).clear();
            children.subList(mid + 1, children.size()).clear();
        }

        return newNode;
    }

    /**
     * 获取节点的第一个键
     * 用于非叶子节点的导航键
     *
     * @return 第一个键
     */
    public K getFirstKey() {
        if (keys.isEmpty()) {
            return null;
        }
        return keys.get(0);
    }

    /**
     * 获取节点的最后一个键
     *
     * @return 最后一个键
     */
    public K getLastKey() {
        if (keys.isEmpty()) {
            return null;
        }
        return keys.get(keys.size() - 1);
    }

    /**
     * 判断节点是否为根节点
     *
     * @return true 表示根节点
     */
    public boolean isRoot() {
        return parent == null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(isLeaf() ? "LeafNode" : "InternalNode");
        sb.append("{keys=").append(keys);
        if (isLeaf() && values != null) {
            sb.append(", values=").append(values);
        }
        sb.append("}");
        return sb.toString();
    }
}
