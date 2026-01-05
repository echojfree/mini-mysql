package com.minidb.storage.buffer;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * LRU 替换策略（Least Recently Used）
 *
 * LRU 是一种常用的缓存替换算法，优先驱逐最久未使用的页面
 *
 * 实现方式：
 * 1. 使用双向链表维护访问顺序
 * 2. 最近访问的页面放在链表头部
 * 3. 驱逐时从链表尾部选择页面
 *
 * 为什么选择 LRU？
 * 1. 时间局部性原理：最近使用的数据很可能再次被使用
 * 2. 实现简单，性能稳定
 * 3. 符合大多数数据库访问模式
 *
 * LRU 的优化：
 * 1. LRU-K：考虑最近 K 次访问的时间
 * 2. 2Q：使用两个队列分别管理热数据和冷数据
 * 3. ARC：自适应替换缓存
 *
 * 对应八股文知识点：
 * ✅ LRU 替换算法的原理
 * ✅ LRU 的实现方式（双向链表 + 哈希表）
 * ✅ LRU 的时间复杂度：O(1) 访问，O(1) 更新
 * ✅ LRU 的优缺点
 * ✅ 数据库中为什么使用 LRU？
 *
 * @author Mini-MySQL
 */
@Slf4j
public class LRUReplacer {

    /**
     * 双向链表节点
     */
    private static class Node {
        int frameId;
        Node prev;
        Node next;

        Node(int frameId) {
            this.frameId = frameId;
        }
    }

    /**
     * 链表头节点（哨兵节点）
     * 最近使用的节点在头部
     */
    private final Node head;

    /**
     * 链表尾节点（哨兵节点）
     * 最久未使用的节点在尾部
     */
    private final Node tail;

    /**
     * 哈希表：frameId -> Node
     * 用于 O(1) 时间查找节点
     */
    private final ConcurrentHashMap<Integer, Node> map;

    /**
     * 当前可驱逐的页面数量
     */
    private int size;

    /**
     * 最大容量
     */
    private final int capacity;

    /**
     * 锁，保护链表操作
     */
    private final ReentrantLock lock;

    /**
     * 构造函数
     *
     * @param capacity 最大容量
     */
    public LRUReplacer(int capacity) {
        this.capacity = capacity;
        this.size = 0;
        this.map = new ConcurrentHashMap<>();
        this.lock = new ReentrantLock();

        // 初始化哨兵节点
        this.head = new Node(-1);
        this.tail = new Node(-1);
        head.next = tail;
        tail.prev = head;
    }

    /**
     * 选择一个页面进行驱逐
     * 返回最久未使用的页面的 frameId
     *
     * @return frameId，如果没有可驱逐的页面返回 -1
     */
    public int victim() {
        lock.lock();
        try {
            // 如果没有可驱逐的页面
            if (size == 0) {
                return -1;
            }

            // 从链表尾部（最久未使用）获取节点
            Node victim = tail.prev;
            if (victim == head) {
                return -1;
            }

            // 从链表中移除
            removeNode(victim);
            map.remove(victim.frameId);
            size--;

            log.debug("LRU victim: frameId={}, remaining size={}", victim.frameId, size);
            return victim.frameId;

        } finally {
            lock.unlock();
        }
    }

    /**
     * 标记页面为可驱逐
     * 当页面的 pin_count 降为 0 时调用
     *
     * @param frameId 帧 ID
     */
    public void pin(int frameId) {
        lock.lock();
        try {
            // 如果已经在链表中，先移除
            if (map.containsKey(frameId)) {
                Node node = map.get(frameId);
                removeNode(node);
                map.remove(frameId);
                size--;
            }

            log.debug("LRU pin: frameId={}, size={}", frameId, size);

        } finally {
            lock.unlock();
        }
    }

    /**
     * 标记页面为不可驱逐
     * 当页面被访问时调用（pin_count > 0）
     *
     * @param frameId 帧 ID
     */
    public void unpin(int frameId) {
        lock.lock();
        try {
            // 如果已经在链表中，先移除（避免重复）
            if (map.containsKey(frameId)) {
                return;
            }

            // 检查容量
            if (size >= capacity) {
                log.warn("LRU replacer is full, cannot unpin frameId={}", frameId);
                return;
            }

            // 创建新节点并添加到链表头部（最近使用）
            Node node = new Node(frameId);
            addToHead(node);
            map.put(frameId, node);
            size++;

            log.debug("LRU unpin: frameId={}, size={}", frameId, size);

        } finally {
            lock.unlock();
        }
    }

    /**
     * 从链表中移除节点
     *
     * @param node 要移除的节点
     */
    private void removeNode(Node node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    /**
     * 将节点添加到链表头部
     *
     * @param node 要添加的节点
     */
    private void addToHead(Node node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }

    /**
     * 获取当前可驱逐的页面数量
     *
     * @return 页面数量
     */
    public int getSize() {
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 清空替换器
     */
    public void clear() {
        lock.lock();
        try {
            map.clear();
            head.next = tail;
            tail.prev = head;
            size = 0;
            log.debug("LRU replacer cleared");
        } finally {
            lock.unlock();
        }
    }
}
