package com.minidb.storage.buffer;

import com.minidb.storage.file.DiskManager;
import com.minidb.storage.page.Page;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 缓冲池（Buffer Pool）
 *
 * 缓冲池是数据库系统的核心组件，负责在内存中缓存磁盘页面
 *
 * 核心功能：
 * 1. 页面缓存：将磁盘页面缓存到内存，减少磁盘 I/O
 * 2. 页面管理：管理页面的加载、驱逐和刷盘
 * 3. 并发控制：支持多线程并发访问
 *
 * 工作流程：
 * 1. fetchPage()：请求页面
 *    - 如果页面在缓冲池中，直接返回
 *    - 如果页面不在，从磁盘加载到空闲帧
 *    - 如果没有空闲帧，使用 LRU 驱逐一个页面
 *
 * 2. unpinPage()：释放页面
 *    - 减少 pin_count
 *    - 如果 pin_count = 0，页面变为可驱逐
 *
 * 3. flushPage()：刷盘
 *    - 将脏页写回磁盘
 *    - 清除脏页标记
 *
 * 为什么需要 Buffer Pool？
 * 1. 性能提升：内存访问比磁盘快 1000-10000 倍
 * 2. 减少 I/O：批量刷盘，合并写操作
 * 3. 缓存热数据：利用数据访问的局部性原理
 *
 * Buffer Pool 的关键指标：
 * 1. 命中率（Hit Ratio）：(命中次数 / 总请求次数) * 100%
 *    - 命中率越高，性能越好
 *    - 生产环境目标：> 95%
 *
 * 2. 脏页率（Dirty Page Ratio）：脏页数 / 总页数
 *    - 脏页率过高会导致刷盘压力大
 *
 * 对应八股文知识点：
 * ✅ Buffer Pool 的作用
 * ✅ Buffer Pool 的工作原理
 * ✅ 页面驱逐策略（LRU）
 * ✅ 脏页的刷盘时机
 * ✅ Buffer Pool 的并发控制
 * ✅ Buffer Pool 的命中率优化
 *
 * @author Mini-MySQL
 */
@Slf4j
@Getter
public class BufferPool {

    /**
     * 缓冲池大小（帧的数量）
     */
    private final int poolSize;

    /**
     * 帧数组
     * 索引对应 frameId
     */
    private final Frame[] frames;

    /**
     * 页面表（Page Table）
     * Key: pageId（格式：spaceId:pageNumber）
     * Value: frameId
     * 用于快速查找页面是否在缓冲池中
     */
    private final ConcurrentHashMap<String, Integer> pageTable;

    /**
     * 空闲帧列表
     * 存储空闲帧的 frameId
     */
    private final ConcurrentHashMap<Integer, Boolean> freeList;

    /**
     * LRU 替换器
     * 用于选择驱逐页面
     */
    private final LRUReplacer replacer;

    /**
     * 磁盘管理器
     * 用于读写磁盘页面
     */
    private final DiskManager diskManager;

    /**
     * 全局锁
     * 保护缓冲池的元数据（pageTable, freeList 等）
     */
    private final ReentrantLock globalLock;

    /**
     * 统计信息：请求次数
     */
    private long requestCount;

    /**
     * 统计信息：命中次数
     */
    private long hitCount;

    /**
     * 统计信息：驱逐次数
     */
    private long evictionCount;

    /**
     * 构造函数
     *
     * @param poolSize    缓冲池大小
     * @param diskManager 磁盘管理器
     */
    public BufferPool(int poolSize, DiskManager diskManager) {
        if (poolSize <= 0) {
            throw new IllegalArgumentException("Pool size must be positive");
        }

        this.poolSize = poolSize;
        this.diskManager = diskManager;
        this.frames = new Frame[poolSize];
        this.pageTable = new ConcurrentHashMap<>();
        this.freeList = new ConcurrentHashMap<>();
        this.replacer = new LRUReplacer(poolSize);
        this.globalLock = new ReentrantLock();
        this.requestCount = 0;
        this.hitCount = 0;
        this.evictionCount = 0;

        // 初始化所有帧
        for (int i = 0; i < poolSize; i++) {
            frames[i] = new Frame(i);
            freeList.put(i, true);
        }

        log.info("BufferPool initialized: poolSize={}", poolSize);
    }

    /**
     * 获取页面
     * 这是缓冲池的核心方法
     *
     * @param spaceId    表空间 ID
     * @param pageNumber 页号
     * @return 页面对象
     * @throws IOException 磁盘 I/O 异常
     */
    public Page fetchPage(int spaceId, int pageNumber) throws IOException {
        String pageId = makePageId(spaceId, pageNumber);

        globalLock.lock();
        try {
            requestCount++;

            // 1. 检查页面是否已经在缓冲池中
            if (pageTable.containsKey(pageId)) {
                int frameId = pageTable.get(pageId);
                Frame frame = frames[frameId];

                // 命中！增加 pin_count
                frame.pin();
                replacer.pin(frameId);  // 从 LRU 中移除（不可驱逐）
                hitCount++;

                log.debug("Buffer pool HIT: spaceId={}, pageNumber={}, frameId={}, hitRate={:.2f}%",
                        spaceId, pageNumber, frameId, getHitRate() * 100);

                return frame.getPage();
            }

            // 2. 页面不在缓冲池中，需要从磁盘加载
            // 2.1 获取一个空闲帧
            int frameId = getVictimFrame();
            if (frameId == -1) {
                throw new IOException("No available frame in buffer pool");
            }

            Frame frame = frames[frameId];

            // 2.2 如果帧中有旧页面且是脏页，先刷盘
            if (!frame.isFree() && frame.isDirty()) {
                flushFrame(frame);
            }

            // 2.3 从磁盘加载新页面
            Page page = diskManager.readPage(spaceId, pageNumber);

            // 2.4 将页面放入帧中
            frame.setPage(page, spaceId, pageNumber);
            frame.pin();

            // 2.5 更新页面表
            if (!frame.isFree()) {
                // 移除旧页面的映射
                String oldPageId = frame.getPageId();
                pageTable.remove(oldPageId);
            }
            pageTable.put(pageId, frameId);

            // 2.6 从空闲列表中移除
            freeList.remove(frameId);

            log.debug("Buffer pool MISS: loaded spaceId={}, pageNumber={} to frameId={}, hitRate={:.2f}%",
                    spaceId, pageNumber, frameId, getHitRate() * 100);

            return page;

        } finally {
            globalLock.unlock();
        }
    }

    /**
     * 释放页面
     * 当线程使用完页面后调用
     *
     * @param spaceId    表空间 ID
     * @param pageNumber 页号
     * @param isDirty    是否修改过页面
     */
    public void unpinPage(int spaceId, int pageNumber, boolean isDirty) {
        String pageId = makePageId(spaceId, pageNumber);

        globalLock.lock();
        try {
            // 检查页面是否在缓冲池中
            if (!pageTable.containsKey(pageId)) {
                log.warn("Trying to unpin a page not in buffer pool: {}", pageId);
                return;
            }

            int frameId = pageTable.get(pageId);
            Frame frame = frames[frameId];

            // 减少 pin_count
            frame.unpin();

            // 如果页面被修改，标记为脏页
            if (isDirty) {
                frame.markDirty();
            }

            // 如果 pin_count = 0，页面可以被驱逐
            if (frame.isEvictable()) {
                replacer.unpin(frameId);  // 加入 LRU（可驱逐）
            }

            log.debug("Unpinned page: spaceId={}, pageNumber={}, frameId={}, pinCount={}, dirty={}",
                    spaceId, pageNumber, frameId, frame.getPinCount(), frame.isDirty());

        } finally {
            globalLock.unlock();
        }
    }

    /**
     * 刷盘单个页面
     *
     * @param spaceId    表空间 ID
     * @param pageNumber 页号
     * @throws IOException 磁盘 I/O 异常
     */
    public void flushPage(int spaceId, int pageNumber) throws IOException {
        String pageId = makePageId(spaceId, pageNumber);

        globalLock.lock();
        try {
            if (!pageTable.containsKey(pageId)) {
                log.warn("Trying to flush a page not in buffer pool: {}", pageId);
                return;
            }

            int frameId = pageTable.get(pageId);
            Frame frame = frames[frameId];

            flushFrame(frame);

        } finally {
            globalLock.unlock();
        }
    }

    /**
     * 刷盘所有脏页
     *
     * @throws IOException 磁盘 I/O 异常
     */
    public void flushAllPages() throws IOException {
        globalLock.lock();
        try {
            int flushedCount = 0;
            for (Frame frame : frames) {
                if (!frame.isFree() && frame.isDirty()) {
                    flushFrame(frame);
                    flushedCount++;
                }
            }
            log.info("Flushed all dirty pages: count={}", flushedCount);

        } finally {
            globalLock.unlock();
        }
    }

    /**
     * 刷盘一个帧
     *
     * @param frame 帧对象
     * @throws IOException 磁盘 I/O 异常
     */
    private void flushFrame(Frame frame) throws IOException {
        if (frame.isFree() || !frame.isDirty()) {
            return;
        }

        // 更新页面的 checksum 和 LSN
        Page page = frame.getPage();
        long newLsn = page.getHeader().getLastModifyLsn() + 1;
        page.updateChecksumAndLsn(newLsn);

        // 写回磁盘
        diskManager.writePage(frame.getSpaceId(), page);
        frame.setDirty(false);

        log.debug("Flushed dirty page: spaceId={}, pageNumber={}",
                frame.getSpaceId(), frame.getPageNumber());
    }

    /**
     * 获取一个可用的帧
     * 优先使用空闲帧，如果没有则使用 LRU 驱逐一个页面
     *
     * @return frameId，如果失败返回 -1
     * @throws IOException 磁盘 I/O 异常
     */
    private int getVictimFrame() throws IOException {
        // 1. 优先使用空闲帧
        for (Integer frameId : freeList.keySet()) {
            return frameId;
        }

        // 2. 使用 LRU 驱逐一个页面
        int victimFrameId = replacer.victim();
        if (victimFrameId != -1) {
            evictionCount++;
            log.debug("Evicted page from frameId={}, total evictions={}", victimFrameId, evictionCount);
        }

        return victimFrameId;
    }

    /**
     * 生成页面 ID
     *
     * @param spaceId    表空间 ID
     * @param pageNumber 页号
     * @return 页面 ID
     */
    private String makePageId(int spaceId, int pageNumber) {
        return spaceId + ":" + pageNumber;
    }

    /**
     * 获取缓冲池命中率
     *
     * @return 命中率（0.0-1.0）
     */
    public double getHitRate() {
        if (requestCount == 0) {
            return 0.0;
        }
        return (double) hitCount / requestCount;
    }

    /**
     * 获取脏页数量
     *
     * @return 脏页数量
     */
    public int getDirtyPageCount() {
        int count = 0;
        for (Frame frame : frames) {
            if (!frame.isFree() && frame.isDirty()) {
                count++;
            }
        }
        return count;
    }

    /**
     * 获取已使用的帧数量
     *
     * @return 已使用帧数量
     */
    public int getUsedFrameCount() {
        return poolSize - freeList.size();
    }

    /**
     * 打印缓冲池统计信息
     */
    public void printStats() {
        log.info("=== Buffer Pool Statistics ===");
        log.info("Pool Size: {}", poolSize);
        log.info("Used Frames: {}/{}", getUsedFrameCount(), poolSize);
        log.info("Dirty Pages: {}", getDirtyPageCount());
        log.info("Request Count: {}", requestCount);
        log.info("Hit Count: {}", hitCount);
        log.info("Hit Rate: {:.2f}%", getHitRate() * 100);
        log.info("Eviction Count: {}", evictionCount);
        log.info("============================");
    }
}
