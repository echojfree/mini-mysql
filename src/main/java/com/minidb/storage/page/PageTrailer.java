package com.minidb.storage.page;

import lombok.Data;

import java.nio.ByteBuffer;

/**
 * Page Trailer (页尾)
 *
 * MySQL InnoDB 页尾结构 (8 字节):
 * - FIL_PAGE_END_LSN (8 字节): 页面最后 8 字节的 LSN
 *
 * 作用:
 * 1. 校验页面完整性 (页头和页尾的 LSN 应该一致)
 * 2. 崩溃恢复时判断页面是否完整写入
 *
 * 对应八股文知识点:
 * ✅ 页尾的作用
 * ✅ Double Write Buffer (解决部分写问题)
 * ✅ 页面完整性校验
 *
 * @author Mini-MySQL
 */
@Data
public class PageTrailer {

    /**
     * 页尾固定大小: 8 字节
     */
    public static final int PAGE_TRAILER_SIZE = 8;

    /**
     * 页面 LSN 的后 4 字节
     * 用于校验页面完整性
     */
    private long checksum;

    /**
     * 默认构造函数
     */
    public PageTrailer() {
        this.checksum = 0;
    }

    /**
     * 构造函数
     *
     * @param lsn 页面的 LSN
     */
    public PageTrailer(long lsn) {
        // 简化实现: 直接使用 LSN 作为校验值
        // 实际 MySQL 使用更复杂的校验算法 (Checksum)
        this.checksum = lsn;
    }

    /**
     * 序列化到 ByteBuffer
     *
     * @param buffer 目标缓冲区
     */
    public void serialize(ByteBuffer buffer) {
        buffer.putLong(checksum); // 8 字节
    }

    /**
     * 从 ByteBuffer 反序列化
     *
     * @param buffer 源缓冲区
     * @return PageTrailer 对象
     */
    public static PageTrailer deserialize(ByteBuffer buffer) {
        PageTrailer trailer = new PageTrailer();
        trailer.checksum = buffer.getLong();
        return trailer;
    }

    /**
     * 验证页面完整性
     *
     * @param pageLsn 页头中的 LSN
     * @return true 如果校验通过
     */
    public boolean verify(long pageLsn) {
        return this.checksum == pageLsn;
    }

    @Override
    public String toString() {
        return String.format("PageTrailer{checksum=%d}", checksum);
    }
}
