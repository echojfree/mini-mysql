package com.minidb.storage.page;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;

import static com.minidb.common.Constants.*;

/**
 * 页面（Page）
 * 大小：16KB (16384 字节)
 *
 * 页面是 InnoDB 存储引擎管理磁盘和内存的基本单位。
 * 所有的数据都以页面为单位在磁盘和内存之间传输。
 *
 * 页面结构：
 * +------------------+------------+
 * | PageHeader       | 38 字节    |
 * +------------------+------------+
 * | 用户数据区       | 16338 字节 |
 * +------------------+------------+
 * | PageTrailer      | 8 字节     |
 * +------------------+------------+
 * 总计：16384 字节（16KB）
 *
 * 核心功能：
 * 1. 数据存储：在数据区存储用户数据或索引数据
 * 2. 序列化/反序列化：页面与字节数组之间的转换
 * 3. 完整性校验：通过 checksum 检测页面损坏
 * 4. 双向链表：通过 previousPage/nextPage 构建页面链表
 *
 * 关键设计：
 * - 固定大小（16KB）：便于内存管理和磁盘 I/O
 * - 页头+页尾：存储元数据和完整性校验信息
 * - 支持不同类型：数据页、索引页、Undo 页等
 *
 * @author Mini-MySQL
 */
@Data
@Slf4j
public class Page {

    /**
     * 页头：存储页面元数据
     */
    private PageHeader header;

    /**
     * 页尾：存储完整性校验信息
     */
    private PageTrailer trailer;

    /**
     * 用户数据区：存储实际数据
     * 大小：16384 - 38 - 8 = 16338 字节
     */
    private byte[] data;

    /**
     * 标记页面是否被修改（脏页）
     * 脏页需要在合适的时机刷盘
     */
    private boolean dirty;

    /**
     * 构造函数：创建一个新的空白页面
     */
    public Page() {
        this.header = new PageHeader();
        this.trailer = new PageTrailer();
        this.data = new byte[PAGE_DATA_SIZE];
        this.dirty = false;
    }

    /**
     * 构造函数：创建指定类型和页号的页面
     *
     * @param pageType   页面类型
     * @param pageNumber 页号
     * @param spaceId    表空间 ID
     */
    public Page(byte pageType, int pageNumber, int spaceId) {
        this();
        this.header.setPageType(pageType);
        this.header.setPageNumber(pageNumber);
        this.header.setSpaceId(spaceId);
    }

    /**
     * 写入数据到页面
     *
     * @param offset 数据区偏移量（相对于数据区起始位置）
     * @param bytes  要写入的数据
     * @return 是否写入成功
     */
    public boolean writeData(int offset, byte[] bytes) {
        if (offset < 0 || offset + bytes.length > PAGE_DATA_SIZE) {
            log.error("Write data out of bounds: offset={}, length={}, pageDataSize={}",
                    offset, bytes.length, PAGE_DATA_SIZE);
            return false;
        }

        // 将数据写入数据区
        System.arraycopy(bytes, 0, this.data, offset, bytes.length);

        // 标记为脏页
        this.dirty = true;

        log.debug("Write data to page {}: offset={}, length={}",
                header.getPageNumber(), offset, bytes.length);

        return true;
    }

    /**
     * 从页面读取数据
     *
     * @param offset 数据区偏移量
     * @param length 读取长度
     * @return 读取的数据
     */
    public byte[] readData(int offset, int length) {
        if (offset < 0 || offset + length > PAGE_DATA_SIZE) {
            log.error("Read data out of bounds: offset={}, length={}, pageDataSize={}",
                    offset, length, PAGE_DATA_SIZE);
            throw new IllegalArgumentException("Read out of bounds");
        }

        byte[] result = new byte[length];
        System.arraycopy(this.data, offset, result, 0, length);

        log.debug("Read data from page {}: offset={}, length={}",
                header.getPageNumber(), offset, length);

        return result;
    }

    /**
     * 计算页面的 CRC32 校验和
     * 校验范围：页头（不含 checksum 字段）+ 数据区
     *
     * @return CRC32 校验和
     */
    public int calculateChecksum() {
        CRC32 crc32 = new CRC32();

        // 序列化页头（暂时将 checksum 设为 0）
        int originalChecksum = header.getChecksum();
        header.setChecksum(0);
        byte[] headerBytes = header.serialize();
        header.setChecksum(originalChecksum);

        // 计算页头的校验和
        crc32.update(headerBytes);

        // 计算数据区的校验和
        crc32.update(data);

        return (int) crc32.getValue();
    }

    /**
     * 更新页面的校验和和 LSN
     * 在页面修改后调用，准备写入磁盘
     *
     * @param lsn 当前的 LSN（Log Sequence Number）
     */
    public void updateChecksumAndLsn(long lsn) {
        // 更新 LSN
        header.setLastModifyLsn(lsn);

        // 计算并更新校验和
        int checksum = calculateChecksum();
        header.setChecksum(checksum);

        // 更新页尾
        trailer.setChecksum(checksum);
        trailer.setLsnLowPart(PageTrailer.extractLsnLowPart(lsn));

        log.debug("Update checksum and LSN for page {}: checksum={}, lsn={}",
                header.getPageNumber(), checksum, lsn);
    }

    /**
     * 校验页面完整性
     * 检查页头和页尾的校验和、LSN 是否一致
     *
     * @return true 表示页面完整，false 表示页面损坏
     */
    public boolean verify() {
        // 1. 检查页头和页尾的校验和是否一致
        if (header.getChecksum() != trailer.getChecksum()) {
            log.error("Page {} checksum mismatch: header={}, trailer={}",
                    header.getPageNumber(), header.getChecksum(), trailer.getChecksum());
            return false;
        }

        // 2. 检查 LSN 低 4 字节是否一致
        int expectedLsnLowPart = PageTrailer.extractLsnLowPart(header.getLastModifyLsn());
        if (expectedLsnLowPart != trailer.getLsnLowPart()) {
            log.error("Page {} LSN mismatch: expected={}, actual={}",
                    header.getPageNumber(), expectedLsnLowPart, trailer.getLsnLowPart());
            return false;
        }

        // 3. 重新计算校验和，验证数据是否被篡改
        int calculatedChecksum = calculateChecksum();
        if (calculatedChecksum != header.getChecksum()) {
            log.error("Page {} data corrupted: calculated={}, stored={}",
                    header.getPageNumber(), calculatedChecksum, header.getChecksum());
            return false;
        }

        log.debug("Page {} verification passed", header.getPageNumber());
        return true;
    }

    /**
     * 将页面序列化为字节数组（16KB）
     * 格式：PageHeader (38) + Data (16338) + PageTrailer (8)
     *
     * @return 16KB 的字节数组
     */
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);

        // 1. 写入页头（38 字节）
        buffer.put(header.serialize());

        // 2. 写入数据区（16338 字节）
        buffer.put(data);

        // 3. 写入页尾（8 字节）
        buffer.put(trailer.serialize());

        byte[] result = buffer.array();

        log.debug("Serialize page {}: size={}", header.getPageNumber(), result.length);

        return result;
    }

    /**
     * 从字节数组反序列化页面
     *
     * @param bytes 16KB 的字节数组
     * @return Page 对象
     */
    public static Page deserialize(byte[] bytes) {
        if (bytes.length != PAGE_SIZE) {
            throw new IllegalArgumentException("Invalid page size: " + bytes.length);
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // 1. 读取页头（38 字节）
        byte[] headerBytes = new byte[PAGE_HEADER_SIZE];
        buffer.get(headerBytes);
        PageHeader header = PageHeader.deserialize(headerBytes);

        // 2. 读取数据区（16338 字节）
        byte[] data = new byte[PAGE_DATA_SIZE];
        buffer.get(data);

        // 3. 读取页尾（8 字节）
        byte[] trailerBytes = new byte[PAGE_TRAILER_SIZE];
        buffer.get(trailerBytes);
        PageTrailer trailer = PageTrailer.deserialize(trailerBytes);

        // 4. 创建 Page 对象
        Page page = new Page();
        page.setHeader(header);
        page.setData(data);
        page.setTrailer(trailer);
        page.setDirty(false);

        log.debug("Deserialize page {}", header.getPageNumber());

        return page;
    }

    /**
     * 清空页面数据
     */
    public void clear() {
        this.header = new PageHeader();
        this.trailer = new PageTrailer();
        Arrays.fill(this.data, (byte) 0);
        this.dirty = false;

        log.debug("Clear page");
    }

    /**
     * 获取页面剩余可用空间
     *
     * @return 剩余空间大小（字节）
     */
    public int getFreeSpace() {
        return header.getFreeSpace();
    }

    /**
     * 判断页面是否有足够的空间写入数据
     *
     * @param requiredSize 需要的空间大小
     * @return true 表示空间足够
     */
    public boolean hasEnoughSpace(int requiredSize) {
        return header.getFreeSpace() >= requiredSize;
    }
}
