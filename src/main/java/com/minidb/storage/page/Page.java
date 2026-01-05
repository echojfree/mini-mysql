package com.minidb.storage.page;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Page (页)
 *
 * MySQL InnoDB 页结构 (16KB = 16384 字节):
 * +-------------------+
 * | Page Header (38B) |  页头
 * +-------------------+
 * | User Records      |  用户记录 (数据)
 * | (16KB - 46B)      |
 * +-------------------+
 * | Page Trailer (8B) |  页尾
 * +-------------------+
 *
 * 对应八股文知识点:
 * ✅ 为什么页大小是 16KB?
 *    - 权衡磁盘 I/O 效率和内存使用
 *    - 太小: I/O 次数多
 *    - 太大: 内存浪费,缓存命中率低
 *    - 16KB 是经验值,在大多数场景下表现良好
 *
 * ✅ 页是 InnoDB 的最小 I/O 单位
 *    - 即使只读 1 行数据,也要读取整页
 *    - 缓冲池以页为单位缓存数据
 *
 * @author Mini-MySQL
 */
@Slf4j
@Getter
public class Page {

    /**
     * 页大小: 16KB = 16384 字节
     * MySQL InnoDB 默认页大小
     */
    public static final int PAGE_SIZE = 16 * 1024;

    /**
     * 用户数据区大小 = 总大小 - 页头 - 页尾
     */
    public static final int USER_DATA_SIZE =
            PAGE_SIZE - PageHeader.PAGE_HEADER_SIZE - PageTrailer.PAGE_TRAILER_SIZE;

    /**
     * 页头
     */
    private final PageHeader header;

    /**
     * 用户数据区
     */
    private final byte[] data;

    /**
     * 页尾
     */
    private final PageTrailer trailer;

    /**
     * 脏页标志 (是否被修改过但未刷盘)
     */
    private boolean dirty;

    /**
     * 构造函数 - 创建新页
     *
     * @param spaceId 表空间 ID
     * @param pageNo 页号
     * @param pageType 页类型
     */
    public Page(int spaceId, int pageNo, PageType pageType) {
        this.header = new PageHeader(spaceId, pageNo, pageType);
        this.data = new byte[USER_DATA_SIZE];
        this.trailer = new PageTrailer();
        this.dirty = false;

        log.debug("Created new page: space={}, pageNo={}, type={}", spaceId, pageNo, pageType);
    }

    /**
     * 私有构造函数 - 用于反序列化
     */
    private Page(PageHeader header, byte[] data, PageTrailer trailer) {
        this.header = header;
        this.data = data;
        this.trailer = trailer;
        this.dirty = false;
    }

    /**
     * 写入数据到页面
     *
     * @param offset 偏移量 (相对于用户数据区起始位置)
     * @param bytes 要写入的数据
     * @throws IllegalArgumentException 如果偏移量或数据大小非法
     */
    public void writeData(int offset, byte[] bytes) {
        if (offset < 0 || offset + bytes.length > USER_DATA_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Invalid write: offset=%d, length=%d, max=%d",
                            offset, bytes.length, USER_DATA_SIZE));
        }

        System.arraycopy(bytes, 0, data, offset, bytes.length);
        markDirty();

        log.trace("Wrote {} bytes to page {} at offset {}", bytes.length, header.getPageNo(), offset);
    }

    /**
     * 从页面读取数据
     *
     * @param offset 偏移量
     * @param length 读取长度
     * @return 读取的数据
     */
    public byte[] readData(int offset, int length) {
        if (offset < 0 || offset + length > USER_DATA_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Invalid read: offset=%d, length=%d, max=%d",
                            offset, length, USER_DATA_SIZE));
        }

        byte[] result = new byte[length];
        System.arraycopy(data, offset, result, 0, length);

        log.trace("Read {} bytes from page {} at offset {}", length, header.getPageNo(), offset);
        return result;
    }

    /**
     * 标记为脏页
     */
    public void markDirty() {
        this.dirty = true;
        // 更新 LSN (简化实现: 使用当前时间戳)
        long newLsn = System.currentTimeMillis();
        header.setPageLsn(newLsn);
        trailer.setChecksum(newLsn);
    }

    /**
     * 标记为干净页 (刷盘后)
     */
    public void markClean() {
        this.dirty = false;
    }

    /**
     * 序列化为字节数组 (用于持久化)
     *
     * @return 16KB 字节数组
     */
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);

        // 1. 写入页头 (38 字节)
        header.serialize(buffer);

        // 2. 写入用户数据 (16KB - 46 字节)
        buffer.put(data);

        // 3. 写入页尾 (8 字节)
        trailer.serialize(buffer);

        log.trace("Serialized page {}: size={}", header.getPageNo(), PAGE_SIZE);
        return buffer.array();
    }

    /**
     * 从字节数组反序列化
     *
     * @param bytes 16KB 字节数组
     * @return Page 对象
     * @throws IllegalArgumentException 如果数据格式非法
     */
    public static Page deserialize(byte[] bytes) {
        if (bytes.length != PAGE_SIZE) {
            throw new IllegalArgumentException(
                    "Invalid page size: " + bytes.length + ", expected: " + PAGE_SIZE);
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // 1. 读取页头
        PageHeader header = PageHeader.deserialize(buffer);

        // 2. 读取用户数据
        byte[] data = new byte[USER_DATA_SIZE];
        buffer.get(data);

        // 3. 读取页尾
        PageTrailer trailer = PageTrailer.deserialize(buffer);

        // 4. 校验页面完整性
        if (!trailer.verify(header.getPageLsn())) {
            log.warn("Page checksum mismatch: pageNo={}, headerLsn={}, trailerChecksum={}",
                    header.getPageNo(), header.getPageLsn(), trailer.getChecksum());
        }

        log.debug("Deserialized page {}: type={}", header.getPageNo(), header.getPageType());
        return new Page(header, data, trailer);
    }

    /**
     * 清空页面数据
     */
    public void clear() {
        Arrays.fill(data, (byte) 0);
        markDirty();
    }

    /**
     * 获取页号
     */
    public int getPageNo() {
        return header.getPageNo();
    }

    /**
     * 获取页类型
     */
    public PageType getPageType() {
        return header.getPageType();
    }

    /**
     * 设置页类型
     */
    public void setPageType(PageType type) {
        header.setPageType(type);
        markDirty();
    }

    @Override
    public String toString() {
        return String.format("Page{no=%d, type=%s, dirty=%s, lsn=%d}",
                header.getPageNo(), header.getPageType(), dirty, header.getPageLsn());
    }
}
