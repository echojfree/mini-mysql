package com.minidb.storage.buffer;

import com.minidb.storage.page.Page;
import lombok.Data;

/**
 * Frame (缓冲帧)
 *
 * 功能: 缓冲池中的一个槽位,用于缓存一个页面
 *
 * 结构:
 * - Page: 缓存的页面
 * - 元数据: pageNo, spaceId, isDirty, pinCount, accessTime
 *
 * 对应八股文知识点:
 * ✅ 缓冲帧的概念
 * ✅ Pin机制 (防止页面被换出)
 * ✅ 脏页标记
 * ✅ LRU访问时间
 *
 * @author Mini-MySQL
 */
@Data
public class Frame {

    /**
     * 帧ID (在缓冲池中的位置)
     */
    private final int frameId;

    /**
     * 缓存的页面
     */
    private Page page;

    /**
     * 表空间ID
     */
    private int spaceId;

    /**
     * 页号
     */
    private int pageNo;

    /**
     * Pin计数 (被使用的次数)
     * > 0 表示页面正在被使用,不能被换出
     */
    private int pinCount;

    /**
     * 是否是脏页
     */
    private boolean dirty;

    /**
     * 最后访问时间 (用于LRU)
     */
    private long lastAccessTime;

    /**
     * 构造函数
     */
    public Frame(int frameId) {
        this.frameId = frameId;
        this.page = null;
        this.spaceId = -1;
        this.pageNo = -1;
        this.pinCount = 0;
        this.dirty = false;
        this.lastAccessTime = 0;
    }

    /**
     * 加载页面到帧中
     */
    public void loadPage(Page page) {
        this.page = page;
        this.spaceId = page.getHeader().getSpaceId();
        this.pageNo = page.getPageNo();
        this.dirty = page.isDirty();
        this.lastAccessTime = System.currentTimeMillis();
    }

    /**
     * 清空帧
     */
    public void clear() {
        this.page = null;
        this.spaceId = -1;
        this.pageNo = -1;
        this.pinCount = 0;
        this.dirty = false;
        this.lastAccessTime = 0;
    }

    /**
     * Pin页面 (增加引用计数)
     */
    public void pin() {
        this.pinCount++;
        this.lastAccessTime = System.currentTimeMillis();
    }

    /**
     * Unpin页面 (减少引用计数)
     */
    public void unpin() {
        if (pinCount > 0) {
            pinCount--;
        }
    }

    /**
     * 标记为脏页
     */
    public void markDirty() {
        this.dirty = true;
        if (page != null) {
            page.markDirty();
        }
    }

    /**
     * 是否为空闲帧
     */
    public boolean isFree() {
        return page == null;
    }

    /**
     * 是否可以被换出
     */
    public boolean isEvictable() {
        return pinCount == 0;
    }

    /**
     * 获取页面标识符
     */
    public String getPageId() {
        return spaceId + ":" + pageNo;
    }

    @Override
    public String toString() {
        if (isFree()) {
            return String.format("Frame{id=%d, FREE}", frameId);
        }
        return String.format("Frame{id=%d, page=%d:%d, pin=%d, dirty=%s}",
                frameId, spaceId, pageNo, pinCount, dirty);
    }
}
