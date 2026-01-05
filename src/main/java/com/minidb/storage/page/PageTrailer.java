package com.minidb.storage.page;

import lombok.Data;

import java.nio.ByteBuffer;

import static com.minidb.common.Constants.PAGE_TRAILER_SIZE;

/**
 * 页尾（Page Trailer）
 * 大小：8 字节
 *
 * 页尾用于校验页面完整性，防止页面部分写入（Partial Write）
 *
 * 在 MySQL InnoDB 中，页面写入可能因为断电或系统崩溃而中断，
 * 导致只有部分数据被写入磁盘。页尾通过存储校验和和 LSN，
 * 可以检测这种不完整的写入。
 *
 * 结构说明：
 * +------------------+--------+------------------------------------------------------+
 * | 字段             | 大小   | 说明                                                 |
 * +------------------+--------+------------------------------------------------------+
 * | checksum         | 4 字节 | 页面校验和，与页头的 checksum 相同                    |
 * | lsn              | 4 字节 | LSN 的低 4 字节，用于快速校验                         |
 * +------------------+--------+------------------------------------------------------+
 * 总计：8 字节
 *
 * 完整性校验逻辑：
 * 1. 页头的 checksum == 页尾的 checksum
 * 2. 页头的 lastModifyLsn 低 4 字节 == 页尾的 lsn
 * 如果不相等，说明页面写入不完整，需要从 Redo Log 恢复
 *
 * @author Mini-MySQL
 */
@Data
public class PageTrailer {

    /**
     * 页面校验和（4 字节）
     * 应该与页头的 checksum 字段相同
     * 用于检测页面是否完整写入
     */
    private int checksum;

    /**
     * LSN 的低 4 字节（4 字节）
     * 取页头 lastModifyLsn 的低 4 字节
     * 用于快速检测页面写入是否完整
     *
     * 为什么只存低 4 字节？
     * - 节省空间（页尾只有 8 字节）
     * - 低 4 字节已经足够检测大部分写入错误
     * - 完整的 LSN 已经存储在页头中
     */
    private int lsnLowPart;

    /**
     * 构造函数
     */
    public PageTrailer() {
        this.checksum = 0;
        this.lsnLowPart = 0;
    }

    /**
     * 构造函数
     *
     * @param checksum   校验和
     * @param lsnLowPart LSN 低 4 字节
     */
    public PageTrailer(int checksum, int lsnLowPart) {
        this.checksum = checksum;
        this.lsnLowPart = lsnLowPart;
    }

    /**
     * 将页尾序列化为字节数组
     *
     * @return 8 字节的字节数组
     */
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_TRAILER_SIZE);
        buffer.putInt(checksum);     // 4 字节
        buffer.putInt(lsnLowPart);   // 4 字节
        return buffer.array();
    }

    /**
     * 从字节数组反序列化页尾
     *
     * @param data 字节数组
     * @return PageTrailer 对象
     */
    public static PageTrailer deserialize(byte[] data) {
        if (data.length < PAGE_TRAILER_SIZE) {
            throw new IllegalArgumentException("Invalid page trailer data length: " + data.length);
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int checksum = buffer.getInt();
        int lsnLowPart = buffer.getInt();

        return new PageTrailer(checksum, lsnLowPart);
    }

    /**
     * 从完整的 LSN 中提取低 4 字节
     *
     * @param lsn 完整的 8 字节 LSN
     * @return LSN 的低 4 字节
     */
    public static int extractLsnLowPart(long lsn) {
        return (int) (lsn & 0xFFFFFFFFL);
    }
}
