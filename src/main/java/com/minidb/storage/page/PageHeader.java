package com.minidb.storage.page;

import lombok.Data;

import java.nio.ByteBuffer;

import static com.minidb.common.Constants.*;

/**
 * 页头（Page Header）
 * 大小：38 字节
 *
 * 页头存储页面的元数据信息，是页面管理的核心
 *
 * 结构说明：
 * +------------------+--------+------------------------------------------------------+
 * | 字段             | 大小   | 说明                                                 |
 * +------------------+--------+------------------------------------------------------+
 * | checksum         | 4 字节 | 页面校验和，用于检测页面损坏                          |
 * | pageNumber       | 4 字节 | 页号，页面在表空间中的唯一标识                        |
 * | previousPage     | 4 字节 | 前一个页面的页号（用于双向链表）                      |
 * | nextPage         | 4 字节 | 后一个页面的页号（用于双向链表）                      |
 * | lastModifyLsn    | 8 字节 | 最后修改的 LSN（Log Sequence Number）               |
 * | pageType         | 1 字节 | 页面类型（数据页、索引页、Undo 页等）                |
 * | fileFlushLsn     | 8 字节 | 页面刷盘时的 LSN                                    |
 * | spaceId          | 4 字节 | 表空间 ID                                           |
 * | recordCount      | 2 字节 | 页面中的记录数                                       |
 * | freeSpace        | 2 字节 | 页面剩余空间大小                                     |
 * | heapTop          | 2 字节 | 堆顶指针，指向空闲空间的起始位置                      |
 * | firstRecordOffset| 2 字节 | 第一条记录的偏移量                                   |
 * | lastRecordOffset | 2 字节 | 最后一条记录的偏移量                                 |
 * | directionBit     | 1 字节 | 插入方向标志（顺序插入或随机插入）                    |
 * +------------------+--------+------------------------------------------------------+
 * 总计：38 字节
 *
 * @author Mini-MySQL
 */
@Data
public class PageHeader {

    /**
     * 页面校验和（4 字节）
     * 用于检测页面是否损坏，采用 CRC32 算法
     */
    private int checksum;

    /**
     * 页号（4 字节）
     * 页面在表空间中的唯一标识，从 0 开始递增
     */
    private int pageNumber;

    /**
     * 前一个页面的页号（4 字节）
     * 用于构建双向链表，叶子节点通过此字段连接
     * -1 表示没有前一个页面
     */
    private int previousPage;

    /**
     * 后一个页面的页号（4 字节）
     * 用于构建双向链表，支持范围扫描
     * -1 表示没有后一个页面
     */
    private int nextPage;

    /**
     * 最后修改的 LSN（8 字节）
     * LSN（Log Sequence Number）是日志序列号
     * 用于崩溃恢复和并发控制
     */
    private long lastModifyLsn;

    /**
     * 页面类型（1 字节）
     * 可选值：
     * - PAGE_TYPE_DATA: 数据页
     * - PAGE_TYPE_INDEX: 索引页
     * - PAGE_TYPE_UNDO: Undo 页
     * - PAGE_TYPE_SYSTEM: 系统页
     */
    private byte pageType;

    /**
     * 页面刷盘时的 LSN（8 字节）
     * 记录页面最后一次刷盘时的 LSN
     */
    private long fileFlushLsn;

    /**
     * 表空间 ID（4 字节）
     * 标识页面所属的表空间
     */
    private int spaceId;

    /**
     * 页面中的记录数（2 字节）
     * 当前页面存储的数据行数量
     */
    private short recordCount;

    /**
     * 页面剩余空间大小（2 字节）
     * 页面中可用的空闲空间，单位：字节
     */
    private short freeSpace;

    /**
     * 堆顶指针（2 字节）
     * 指向页面中空闲空间的起始位置
     * 新插入的记录从堆顶开始写入
     */
    private short heapTop;

    /**
     * 第一条记录的偏移量（2 字节）
     * 相对于页面起始位置的偏移
     */
    private short firstRecordOffset;

    /**
     * 最后一条记录的偏移量（2 字节）
     * 相对于页面起始位置的偏移
     */
    private short lastRecordOffset;

    /**
     * 插入方向标志（1 字节）
     * 0：顺序插入（递增）
     * 1：随机插入
     * 用于优化页面分裂策略
     */
    private byte directionBit;

    /**
     * 构造函数
     */
    public PageHeader() {
        // 初始化默认值
        this.previousPage = -1;
        this.nextPage = -1;
        this.recordCount = 0;
        this.freeSpace = (short) PAGE_DATA_SIZE;
        this.heapTop = (short) PAGE_HEADER_SIZE;
        this.firstRecordOffset = 0;
        this.lastRecordOffset = 0;
        this.directionBit = 0;
    }

    /**
     * 将页头序列化为字节数组
     *
     * @return 38 字节的字节数组
     */
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_HEADER_SIZE);

        buffer.putInt(checksum);           // 4 字节
        buffer.putInt(pageNumber);         // 4 字节
        buffer.putInt(previousPage);       // 4 字节
        buffer.putInt(nextPage);           // 4 字节
        buffer.putLong(lastModifyLsn);     // 8 字节
        buffer.put(pageType);              // 1 字节
        buffer.putLong(fileFlushLsn);      // 8 字节
        buffer.putInt(spaceId);            // 4 字节
        buffer.putShort(recordCount);      // 2 字节
        buffer.putShort(freeSpace);        // 2 字节
        buffer.putShort(heapTop);          // 2 字节
        buffer.putShort(firstRecordOffset);// 2 字节
        buffer.putShort(lastRecordOffset); // 2 字节
        buffer.put(directionBit);          // 1 字节

        return buffer.array();
    }

    /**
     * 从字节数组反序列化页头
     *
     * @param data 字节数组
     * @return PageHeader 对象
     */
    public static PageHeader deserialize(byte[] data) {
        if (data.length < PAGE_HEADER_SIZE) {
            throw new IllegalArgumentException("Invalid page header data length: " + data.length);
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        PageHeader header = new PageHeader();

        header.checksum = buffer.getInt();
        header.pageNumber = buffer.getInt();
        header.previousPage = buffer.getInt();
        header.nextPage = buffer.getInt();
        header.lastModifyLsn = buffer.getLong();
        header.pageType = buffer.get();
        header.fileFlushLsn = buffer.getLong();
        header.spaceId = buffer.getInt();
        header.recordCount = buffer.getShort();
        header.freeSpace = buffer.getShort();
        header.heapTop = buffer.getShort();
        header.firstRecordOffset = buffer.getShort();
        header.lastRecordOffset = buffer.getShort();
        header.directionBit = buffer.get();

        return header;
    }
}
