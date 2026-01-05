package com.minidb.storage.page;

import lombok.Data;

import java.nio.ByteBuffer;

/**
 * Page Header (页头)
 *
 * MySQL InnoDB 页头结构 (38 字节):
 * - FIL_PAGE_SPACE (4 字节): 表空间 ID
 * - FIL_PAGE_OFFSET (4 字节): 页号
 * - FIL_PAGE_PREV (4 字节): 上一页页号
 * - FIL_PAGE_NEXT (4 字节): 下一页页号
 * - FIL_PAGE_LSN (8 字节): 最后被修改时的 LSN
 * - FIL_PAGE_TYPE (2 字节): 页类型
 * - FIL_PAGE_FILE_FLUSH_LSN (8 字节): 仅在第一个页中,表示文件至少被刷新到了该 LSN
 * - FIL_PAGE_ARCH_LOG_NO (4 字节): 归档日志序号
 *
 * 对应八股文知识点:
 * ✅ 页头的作用
 * ✅ 页号的概念
 * ✅ 双向链表的实现 (prev/next)
 * ✅ LSN 的作用
 *
 * @author Mini-MySQL
 */
@Data
public class PageHeader {

    /**
     * 页头固定大小: 38 字节
     */
    public static final int PAGE_HEADER_SIZE = 38;

    /**
     * 表空间 ID
     */
    private int spaceId;

    /**
     * 页号 (在表空间中的唯一标识)
     */
    private int pageNo;

    /**
     * 上一页页号 (用于双向链表)
     * -1 表示没有上一页
     */
    private int prevPageNo;

    /**
     * 下一页页号 (用于双向链表)
     * -1 表示没有下一页
     */
    private int nextPageNo;

    /**
     * 页面最后被修改时的 LSN (Log Sequence Number)
     * LSN 单调递增,用于崩溃恢复
     */
    private long pageLsn;

    /**
     * 页类型
     */
    private PageType pageType;

    /**
     * 文件刷新 LSN (仅第一页使用)
     */
    private long fileFlushLsn;

    /**
     * 归档日志序号 (简化实现暂不使用)
     */
    private int archLogNo;

    /**
     * 默认构造函数
     */
    public PageHeader() {
        this.spaceId = 0;
        this.pageNo = 0;
        this.prevPageNo = -1;
        this.nextPageNo = -1;
        this.pageLsn = 0;
        this.pageType = PageType.FREE;
        this.fileFlushLsn = 0;
        this.archLogNo = 0;
    }

    /**
     * 构造函数
     */
    public PageHeader(int spaceId, int pageNo, PageType pageType) {
        this();
        this.spaceId = spaceId;
        this.pageNo = pageNo;
        this.pageType = pageType;
    }

    /**
     * 序列化到 ByteBuffer
     *
     * @param buffer 目标缓冲区
     */
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(spaceId);           // 4 字节
        buffer.putInt(pageNo);            // 4 字节
        buffer.putInt(prevPageNo);        // 4 字节
        buffer.putInt(nextPageNo);        // 4 字节
        buffer.putLong(pageLsn);          // 8 字节
        buffer.putShort((short) pageType.getCode()); // 2 字节
        buffer.putLong(fileFlushLsn);     // 8 字节
        buffer.putInt(archLogNo);         // 4 字节
        // 总计: 38 字节
    }

    /**
     * 从 ByteBuffer 反序列化
     *
     * @param buffer 源缓冲区
     * @return PageHeader 对象
     */
    public static PageHeader deserialize(ByteBuffer buffer) {
        PageHeader header = new PageHeader();

        header.spaceId = buffer.getInt();
        header.pageNo = buffer.getInt();
        header.prevPageNo = buffer.getInt();
        header.nextPageNo = buffer.getInt();
        header.pageLsn = buffer.getLong();
        header.pageType = PageType.fromCode(buffer.getShort());
        header.fileFlushLsn = buffer.getLong();
        header.archLogNo = buffer.getInt();

        return header;
    }

    @Override
    public String toString() {
        return String.format("PageHeader{space=%d, page=%d, type=%s, prev=%d, next=%d, lsn=%d}",
                spaceId, pageNo, pageType, prevPageNo, nextPageNo, pageLsn);
    }
}
