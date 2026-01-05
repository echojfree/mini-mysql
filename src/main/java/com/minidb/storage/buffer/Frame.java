package com.minidb.storage.buffer;

import com.minidb.storage.page.Page;
import lombok.Data;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 缓冲池帧（Frame）
 *
 * Frame 是缓冲池中的基本单元，用于存储一个页面
 *
 * 核心概念：
 * 1. Pin Count（钉住计数）：表示有多少线程正在使用这个页面
 *    - pin_count > 0：页面正在被使用，不能被驱逐
 *    - pin_count = 0：页面空闲，可以被驱逐
 *
 * 2. Dirty Flag（脏页标记）：表示页面是否被修改过
 *    - dirty = true：页面被修改，需要写回磁盘
 *    - dirty = false：页面未修改，可以直接丢弃
 *
 * 3. Page ID：唯一标识一个页面（表空间ID + 页号）
 *
 * 为什么需要 Frame？
 * 1. 内存管理：缓冲池容量有限，需要管理页面的加载和驱逐
 * 2. 并发控制：多个线程可能同时访问同一个页面
 * 3. 性能优化：减少磁盘 I/O，将热点数据保留在内存中
 *
 * 对应八股文知识点：
 * ✅ 什么是 Buffer Pool？
 * ✅ Pin Count 的作用
 * ✅ 脏页是什么？为什么需要脏页标记？
 * ✅ 页面驱逐策略（LRU、Clock 等）
 *
 * @author Mini-MySQL
 */
@Data
public class Frame {

    /**
     * 帧 ID（在缓冲池中的索引位置）
     */
    private final int frameId;

    /**
     * 存储的页面
     * null 表示这个帧是空闲的
     */
    private Page page;

    /**
     * 表空间 ID
     * 与 pageNumber 一起唯一标识一个页面
     */
    private int spaceId;

    /**
     * 页号
     * 与 spaceId 一起唯一标识一个页面
     */
    private int pageNumber;

    /**
     * 钉住计数（Pin Count）
     * 表示有多少个线程正在使用这个页面
     * - 值 > 0：页面正在使用，不能被驱逐
     * - 值 = 0：页面空闲，可以被驱逐
     */
    private int pinCount;

    /**
     * 脏页标记（Dirty Flag）
     * true 表示页面被修改过，需要写回磁盘
     */
    private boolean dirty;

    /**
     * 读写锁
     * 保护帧的并发访问
     */
    private final ReentrantReadWriteLock lock;

    /**
     * 最后访问时间戳
     * 用于 LRU 替换策略
     */
    private long lastAccessTime;

    /**
     * 构造函数
     *
     * @param frameId 帧 ID
     */
    public Frame(int frameId) {
        this.frameId = frameId;
        this.page = null;
        this.spaceId = -1;
        this.pageNumber = -1;
        this.pinCount = 0;
        this.dirty = false;
        this.lock = new ReentrantReadWriteLock();
        this.lastAccessTime = 0;
    }

    /**
     * 判断帧是否空闲
     *
     * @return true 表示空闲
     */
    public boolean isFree() {
        return page == null;
    }

    /**
     * 判断页面是否可以被驱逐
     * 只有 pin_count = 0 的页面才能被驱逐
     *
     * @return true 表示可以驱逐
     */
    public boolean isEvictable() {
        return pinCount == 0;
    }

    /**
     * 增加钉住计数
     * 当线程开始使用页面时调用
     */
    public void pin() {
        lock.writeLock().lock();
        try {
            pinCount++;
            updateAccessTime();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 减少钉住计数
     * 当线程使用完页面时调用
     */
    public void unpin() {
        lock.writeLock().lock();
        try {
            if (pinCount > 0) {
                pinCount--;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 设置页面
     *
     * @param page       页面对象
     * @param spaceId    表空间 ID
     * @param pageNumber 页号
     */
    public void setPage(Page page, int spaceId, int pageNumber) {
        lock.writeLock().lock();
        try {
            this.page = page;
            this.spaceId = spaceId;
            this.pageNumber = pageNumber;
            this.dirty = false;
            updateAccessTime();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 清空帧
     * 释放页面，使帧变为空闲状态
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            this.page = null;
            this.spaceId = -1;
            this.pageNumber = -1;
            this.pinCount = 0;
            this.dirty = false;
            this.lastAccessTime = 0;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 标记为脏页
     * 当页面被修改时调用
     */
    public void markDirty() {
        lock.writeLock().lock();
        try {
            this.dirty = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 更新访问时间
     * 用于 LRU 替换策略
     */
    public void updateAccessTime() {
        this.lastAccessTime = System.currentTimeMillis();
    }

    /**
     * 获取页面标识符
     * 用于在缓冲池中快速查找页面
     *
     * @return 页面标识符（格式：spaceId:pageNumber）
     */
    public String getPageId() {
        return spaceId + ":" + pageNumber;
    }

    @Override
    public String toString() {
        return "Frame{" +
                "frameId=" + frameId +
                ", spaceId=" + spaceId +
                ", pageNumber=" + pageNumber +
                ", pinCount=" + pinCount +
                ", dirty=" + dirty +
                ", free=" + isFree() +
                '}';
    }
}
