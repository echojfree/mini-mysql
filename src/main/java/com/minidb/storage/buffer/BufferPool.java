package com.minidb.storage.buffer;

import com.minidb.storage.file.TableSpace;
import com.minidb.storage.page.Page;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Buffer Pool (缓冲池)
 *
 * 功能: 缓存磁盘页面,减少磁盘I/O
 *
 * 核心概念:
 * 1. Frame: 缓冲帧,每个帧可以缓存一个Page
 * 2. LRU Replacer: 决定哪些页面可以被换出
 * 3. Page Table: spaceId:pageNo -> frameId的映射
 *
 * 工作流程:
 * 1. fetchPage(): 获取页面
 *    - 如果在缓冲池,直接返回(cache hit)
 *    - 如果不在,从磁盘读取并缓存(cache miss)
 * 2. unpinPage(): 释放页面
 *    - 减少pin计数
 *    - 如果pin=0,加入LRU可换出列表
 * 3. flushPage(): 刷新脏页到磁盘
 *
 * 对应八股文知识点:
 * ✅ 缓冲池的作用
 * ✅ LRU替换算法
 * ✅ 脏页刷新机制
 * ✅ 缓存命中率
 * ✅ Pin机制防止页面被换出
 *
 * @author Mini-MySQL
 */
@Slf4j
public class BufferPool {

    /**
     * 缓冲池大小(帧数)
     */
    private final int poolSize;

    /**
     * 所有帧
     */
    private final Frame[] frames;

    /**
     * LRU替换器
     */
    private final LRUReplacer replacer;

    /**
     * 页表: spaceId:pageNo -> frameId
     */
    private final Map<String, Integer> pageTable;

    /**
     * 空闲帧列表
     */
    private final java.util.Queue<Integer> freeList;

    /**
     * 表空间
     */
    private final TableSpace tableSpace;

    /**
     * 锁
     */
    private final ReentrantLock lock;

    /**
     * 统计信息
     */
    @Getter
    private long hitCount = 0;

    @Getter
    private long missCount = 0;

    /**
     * 构造函数
     *
     * @param poolSize 缓冲池大小
     * @param tableSpace 表空间
     */
    public BufferPool(int poolSize, TableSpace tableSpace) {
        this.poolSize = poolSize;
        this.frames = new Frame[poolSize];
        this.replacer = new LRUReplacer(poolSize);
        this.pageTable = new HashMap<>();
        this.freeList = new java.util.LinkedList<>();
        this.tableSpace = tableSpace;
        this.lock = new ReentrantLock();

        // 初始化所有帧
        for (int i = 0; i < poolSize; i++) {
            frames[i] = new Frame(i);
            freeList.offer(i);
        }

        log.info("BufferPool initialized: poolSize={}", poolSize);
    }

    /**
     * 获取页面
     *
     * @param pageNo 页号
     * @return Page对象
     * @throws IOException IO异常
     */
    public Page fetchPage(int pageNo) throws IOException {
        String pageId = tableSpace.getSpaceId() + ":" + pageNo;

        lock.lock();
        try {
            // 1. 检查页表,看页面是否已在缓冲池
            if (pageTable.containsKey(pageId)) {
                // Cache Hit
                hitCount++;
                int frameId = pageTable.get(pageId);
                Frame frame = frames[frameId];

                frame.pin();
                replacer.pin(frameId);

                log.debug("Buffer Hit: pageNo={}, frameId={}, hitRate={:.2f}%",
                        pageNo, frameId, getHitRate() * 100);

                return frame.getPage();
            }

            // Cache Miss
            missCount++;

            // 2. 查找空闲帧
            Integer frameId = findVictimFrame();
            if (frameId == null) {
                throw new RuntimeException("No free frame available");
            }

            Frame frame = frames[frameId];

            // 3. 如果是脏页,先刷盘
            if (frame.isDirty()) {
                flushFrame(frame);
            }

            // 4. 从磁盘读取页面
            Page page = tableSpace.readPage(pageNo);

            // 5. 加载到帧中
            frame.clear();
            frame.loadPage(page);
            frame.pin();

            // 6. 更新页表
            pageTable.put(pageId, frameId);
            replacer.pin(frameId);

            log.debug("Buffer Miss: pageNo={}, frameId={}, hitRate={:.2f}%",
                    pageNo, frameId, getHitRate() * 100);

            return page;

        } finally {
            lock.unlock();
        }
    }

    /**
     * 释放页面
     *
     * @param pageNo 页号
     * @param isDirty 是否为脏页
     */
    public void unpinPage(int pageNo, boolean isDirty) {
        String pageId = tableSpace.getSpaceId() + ":" + pageNo;

        lock.lock();
        try {
            if (!pageTable.containsKey(pageId)) {
                log.warn("Unpin non-existent page: {}", pageNo);
                return;
            }

            int frameId = pageTable.get(pageId);
            Frame frame = frames[frameId];

            if (isDirty) {
                frame.markDirty();
            }

            frame.unpin();

            // 如果没有被pin,加入LRU
            if (frame.isEvictable()) {
                replacer.unpin(frameId);
            }

            log.trace("Unpin page: pageNo={}, frameId={}, pin={}", pageNo, frameId, frame.getPinCount());

        } finally {
            lock.unlock();
        }
    }

    /**
     * 刷新指定页面到磁盘
     */
    public void flushPage(int pageNo) throws IOException {
        String pageId = tableSpace.getSpaceId() + ":" + pageNo;

        lock.lock();
        try {
            if (!pageTable.containsKey(pageId)) {
                return;
            }

            int frameId = pageTable.get(pageId);
            Frame frame = frames[frameId];

            if (frame.isDirty()) {
                flushFrame(frame);
            }

        } finally {
            lock.unlock();
        }
    }

    /**
     * 刷新所有脏页
     */
    public void flushAllPages() throws IOException {
        lock.lock();
        try {
            for (Frame frame : frames) {
                if (!frame.isFree() && frame.isDirty()) {
                    flushFrame(frame);
                }
            }

            log.info("Flushed all dirty pages");

        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取缓存命中率
     */
    public double getHitRate() {
        long total = hitCount + missCount;
        return total == 0 ? 0.0 : (double) hitCount / total;
    }

    /**
     * 获取统计信息
     */
    public String getStats() {
        lock.lock();
        try {
            int usedFrames = pageTable.size();
            int dirtyPages = 0;
            for (Frame frame : frames) {
                if (!frame.isFree() && frame.isDirty()) {
                    dirtyPages++;
                }
            }

            return String.format("BufferPool{size=%d, used=%d, dirty=%d, hits=%d, misses=%d, hitRate=%.2f%%}",
                    poolSize, usedFrames, dirtyPages, hitCount, missCount, getHitRate() * 100);

        } finally {
            lock.unlock();
        }
    }

    /**
     * 查找victim帧
     */
    private Integer findVictimFrame() {
        // 1. 先从空闲列表找
        if (!freeList.isEmpty()) {
            return freeList.poll();
        }

        // 2. 使用LRU找victim
        return replacer.victim();
    }

    /**
     * 刷新帧到磁盘
     */
    private void flushFrame(Frame frame) throws IOException {
        if (frame.isFree()) {
            return;
        }

        tableSpace.writePage(frame.getPage());
        frame.setDirty(false);

        log.debug("Flushed frame: frameId={}, pageNo={}", frame.getFrameId(), frame.getPageNo());
    }

    @Override
    public String toString() {
        return getStats();
    }
}
