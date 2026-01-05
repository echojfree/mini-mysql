package com.minidb.storage.buffer;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * LRU Replacer (LRU替换算法)
 *
 * 功能: 管理缓冲池中哪些页面可以被换出
 *
 * 工作原理:
 * 1. 维护一个双向链表,按访问时间排序
 * 2. 最近访问的在链表头,最久未访问的在链表尾
 * 3. 需要换出时,从链表尾开始查找可换出的帧
 *
 * 对应八股文知识点:
 * ✅ LRU算法原理
 * ✅ 双向链表实现
 * ✅ O(1)时间复杂度的访问和删除
 *
 * @author Mini-MySQL
 */
@Slf4j
public class LRUReplacer {

    /**
     * LRU链表节点
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
     * 最大容量
     */
    private final int capacity;

    /**
     * 虚拟头节点
     */
    private final Node head;

    /**
     * 虚拟尾节点
     */
    private final Node tail;

    /**
     * frameId -> Node 映射
     */
    private final Map<Integer, Node> map;

    /**
     * 当前大小
     */
    private int size;

    /**
     * 锁
     */
    private final ReentrantLock lock;

    public LRUReplacer(int capacity) {
        this.capacity = capacity;
        this.head = new Node(-1);
        this.tail = new Node(-1);
        this.head.next = tail;
        this.tail.prev = head;
        this.map = new HashMap<>();
        this.size = 0;
        this.lock = new ReentrantLock();
    }

    /**
     * 选择一个victim帧换出
     *
     * @return frameId,如果没有可换出的返回-1
     */
    public int victim() {
        lock.lock();
        try {
            if (size == 0) {
                return -1;
            }

            // 从尾部移除(最久未使用)
            Node victim = tail.prev;
            removeNode(victim);
            map.remove(victim.frameId);
            size--;

            log.debug("LRU victim: frameId={}", victim.frameId);
            return victim.frameId;

        } finally {
            lock.unlock();
        }
    }

    /**
     * Pin一个帧(从LRU链表中移除)
     */
    public void pin(int frameId) {
        lock.lock();
        try {
            Node node = map.get(frameId);
            if (node != null) {
                removeNode(node);
                map.remove(frameId);
                size--;
                log.trace("LRU pin: frameId={}, size={}", frameId, size);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Unpin一个帧(加入LRU链表头部)
     */
    public void unpin(int frameId) {
        lock.lock();
        try {
            if (map.containsKey(frameId)) {
                // 已存在,移到头部
                Node node = map.get(frameId);
                removeNode(node);
                addToHead(node);
            } else {
                // 新加入
                Node node = new Node(frameId);
                map.put(frameId, node);
                addToHead(node);
                size++;
            }

            log.trace("LRU unpin: frameId={}, size={}", frameId, size);

        } finally {
            lock.unlock();
        }
    }

    /**
     * 访问一个帧(移到链表头部)
     */
    public void access(int frameId) {
        lock.lock();
        try {
            Node node = map.get(frameId);
            if (node != null) {
                removeNode(node);
                addToHead(node);
                log.trace("LRU access: frameId={}", frameId);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取当前大小
     */
    public int size() {
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从链表中移除节点
     */
    private void removeNode(Node node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    /**
     * 添加节点到链表头部
     */
    private void addToHead(Node node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }

    @Override
    public String toString() {
        lock.lock();
        try {
            List<Integer> frames = new ArrayList<>();
            Node cur = head.next;
            while (cur != tail) {
                frames.add(cur.frameId);
                cur = cur.next;
            }
            return String.format("LRUReplacer{size=%d/%d, frames=%s}",
                    size, capacity, frames);
        } finally {
            lock.unlock();
        }
    }
}
