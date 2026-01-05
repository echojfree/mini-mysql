package com.minidb.storage.page;

import org.junit.jupiter.api.Test;

import static com.minidb.common.Constants.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Page 类的单元测试
 *
 * @author Mini-MySQL
 */
class PageTest {

    /**
     * 测试页面创建
     */
    @Test
    void testPageCreation() {
        Page page = new Page(PAGE_TYPE_DATA, 1, 0);

        assertNotNull(page);
        assertNotNull(page.getHeader());
        assertNotNull(page.getTrailer());
        assertNotNull(page.getData());

        assertEquals(PAGE_TYPE_DATA, page.getHeader().getPageType());
        assertEquals(1, page.getHeader().getPageNumber());
        assertEquals(0, page.getHeader().getSpaceId());
        assertEquals(PAGE_DATA_SIZE, page.getData().length);
        assertFalse(page.isDirty());
    }

    /**
     * 测试数据写入和读取
     */
    @Test
    void testWriteAndReadData() {
        Page page = new Page(PAGE_TYPE_DATA, 1, 0);

        // 写入测试数据
        String testData = "Hello, Mini-MySQL!";
        byte[] dataBytes = testData.getBytes();

        boolean writeResult = page.writeData(0, dataBytes);
        assertTrue(writeResult);
        assertTrue(page.isDirty());

        // 读取数据
        byte[] readBytes = page.readData(0, dataBytes.length);
        assertArrayEquals(dataBytes, readBytes);
        assertEquals(testData, new String(readBytes));
    }

    /**
     * 测试数据写入边界检查
     */
    @Test
    void testWriteDataOutOfBounds() {
        Page page = new Page(PAGE_TYPE_DATA, 1, 0);

        // 尝试写入超出范围的数据
        byte[] largeData = new byte[PAGE_DATA_SIZE + 100];
        boolean result = page.writeData(0, largeData);
        assertFalse(result);

        // 尝试从非法偏移量写入
        byte[] smallData = new byte[100];
        result = page.writeData(-1, smallData);
        assertFalse(result);

        result = page.writeData(PAGE_DATA_SIZE, smallData);
        assertFalse(result);
    }

    /**
     * 测试数据读取边界检查
     */
    @Test
    void testReadDataOutOfBounds() {
        Page page = new Page(PAGE_TYPE_DATA, 1, 0);

        // 尝试读取超出范围的数据
        assertThrows(IllegalArgumentException.class, () -> {
            page.readData(0, PAGE_DATA_SIZE + 100);
        });

        // 尝试从非法偏移量读取
        assertThrows(IllegalArgumentException.class, () -> {
            page.readData(-1, 100);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            page.readData(PAGE_DATA_SIZE, 100);
        });
    }

    /**
     * 测试校验和计算
     */
    @Test
    void testChecksumCalculation() {
        Page page = new Page(PAGE_TYPE_DATA, 1, 0);

        // 写入一些数据
        page.writeData(0, "Test Data".getBytes());

        // 计算校验和
        int checksum1 = page.calculateChecksum();
        assertTrue(checksum1 != 0);

        // 再次计算，应该得到相同的值
        int checksum2 = page.calculateChecksum();
        assertEquals(checksum1, checksum2);

        // 修改数据后，校验和应该不同
        page.writeData(100, "Different Data".getBytes());
        int checksum3 = page.calculateChecksum();
        assertNotEquals(checksum1, checksum3);
    }

    /**
     * 测试校验和和 LSN 更新
     */
    @Test
    void testUpdateChecksumAndLsn() {
        Page page = new Page(PAGE_TYPE_DATA, 1, 0);
        page.writeData(0, "Test Data".getBytes());

        long lsn = 1000L;
        page.updateChecksumAndLsn(lsn);

        // 检查页头
        assertEquals(lsn, page.getHeader().getLastModifyLsn());
        assertTrue(page.getHeader().getChecksum() != 0);

        // 检查页尾
        assertEquals(page.getHeader().getChecksum(), page.getTrailer().getChecksum());
        assertEquals(PageTrailer.extractLsnLowPart(lsn), page.getTrailer().getLsnLowPart());
    }

    /**
     * 测试页面完整性校验
     */
    @Test
    void testPageVerification() {
        Page page = new Page(PAGE_TYPE_DATA, 1, 0);
        page.writeData(0, "Test Data".getBytes());
        page.updateChecksumAndLsn(1000L);

        // 正常情况下，校验应该通过
        assertTrue(page.verify());

        // 篡改页头的校验和
        page.getHeader().setChecksum(999);
        assertFalse(page.verify());

        // 恢复并篡改页尾的校验和
        page.updateChecksumAndLsn(1000L);
        page.getTrailer().setChecksum(999);
        assertFalse(page.verify());

        // 恢复并篡改数据
        page.updateChecksumAndLsn(1000L);
        page.getData()[0] = (byte) 0xFF;
        assertFalse(page.verify());
    }

    /**
     * 测试页面序列化和反序列化
     */
    @Test
    void testSerializeAndDeserialize() {
        // 创建页面并写入数据
        Page originalPage = new Page(PAGE_TYPE_DATA, 100, 5);
        originalPage.getHeader().setPreviousPage(99);
        originalPage.getHeader().setNextPage(101);
        originalPage.getHeader().setRecordCount((short) 10);

        String testData = "Serialization Test Data";
        originalPage.writeData(0, testData.getBytes());
        originalPage.updateChecksumAndLsn(5000L);

        // 序列化
        byte[] serialized = originalPage.serialize();
        assertEquals(PAGE_SIZE, serialized.length);

        // 反序列化
        Page deserializedPage = Page.deserialize(serialized);

        // 验证页头信息
        assertEquals(originalPage.getHeader().getPageType(), deserializedPage.getHeader().getPageType());
        assertEquals(originalPage.getHeader().getPageNumber(), deserializedPage.getHeader().getPageNumber());
        assertEquals(originalPage.getHeader().getSpaceId(), deserializedPage.getHeader().getSpaceId());
        assertEquals(originalPage.getHeader().getPreviousPage(), deserializedPage.getHeader().getPreviousPage());
        assertEquals(originalPage.getHeader().getNextPage(), deserializedPage.getHeader().getNextPage());
        assertEquals(originalPage.getHeader().getRecordCount(), deserializedPage.getHeader().getRecordCount());
        assertEquals(originalPage.getHeader().getLastModifyLsn(), deserializedPage.getHeader().getLastModifyLsn());
        assertEquals(originalPage.getHeader().getChecksum(), deserializedPage.getHeader().getChecksum());

        // 验证数据
        byte[] originalData = originalPage.readData(0, testData.getBytes().length);
        byte[] deserializedData = deserializedPage.readData(0, testData.getBytes().length);
        assertArrayEquals(originalData, deserializedData);

        // 验证页尾
        assertEquals(originalPage.getTrailer().getChecksum(), deserializedPage.getTrailer().getChecksum());
        assertEquals(originalPage.getTrailer().getLsnLowPart(), deserializedPage.getTrailer().getLsnLowPart());

        // 验证完整性
        assertTrue(deserializedPage.verify());
    }

    /**
     * 测试页面清空
     */
    @Test
    void testClearPage() {
        Page page = new Page(PAGE_TYPE_DATA, 1, 0);
        page.writeData(0, "Test Data".getBytes());
        page.updateChecksumAndLsn(1000L);

        assertTrue(page.isDirty());

        // 清空页面
        page.clear();

        // 验证页面已重置
        assertEquals(0, page.getHeader().getPageNumber());
        assertEquals(0, page.getHeader().getChecksum());
        assertEquals(-1, page.getHeader().getPreviousPage());
        assertEquals(-1, page.getHeader().getNextPage());
        assertFalse(page.isDirty());

        // 验证数据区已清空
        byte[] data = page.getData();
        for (byte b : data) {
            assertEquals(0, b);
        }
    }

    /**
     * 测试空间检查
     */
    @Test
    void testSpaceCheck() {
        Page page = new Page(PAGE_TYPE_DATA, 1, 0);

        // 初始状态应该有足够空间
        assertTrue(page.hasEnoughSpace(100));
        assertTrue(page.hasEnoughSpace(PAGE_DATA_SIZE));
        assertFalse(page.hasEnoughSpace(PAGE_DATA_SIZE + 1));

        // 模拟空间减少
        page.getHeader().setFreeSpace((short) 500);
        assertTrue(page.hasEnoughSpace(500));
        assertFalse(page.hasEnoughSpace(501));
    }

    /**
     * 测试 LSN 低 4 字节提取
     */
    @Test
    void testLsnLowPartExtraction() {
        long lsn1 = 0x123456789ABCDEF0L;
        int lowPart1 = PageTrailer.extractLsnLowPart(lsn1);
        assertEquals(0x9ABCDEF0, lowPart1);

        long lsn2 = 0xFFFFFFFFFFFFFFFFL;
        int lowPart2 = PageTrailer.extractLsnLowPart(lsn2);
        assertEquals(0xFFFFFFFF, lowPart2);

        long lsn3 = 0x0000000012345678L;
        int lowPart3 = PageTrailer.extractLsnLowPart(lsn3);
        assertEquals(0x12345678, lowPart3);
    }

    /**
     * 测试并发场景下的页面访问
     * (简单的并发测试，验证基本线程安全性)
     */
    @Test
    void testConcurrentAccess() throws InterruptedException {
        Page page = new Page(PAGE_TYPE_DATA, 1, 0);

        // 创建多个线程同时写入不同位置
        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                page.writeData(i * 10, ("Thread1-" + i).getBytes());
            }
        });

        Thread t2 = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                page.writeData(i * 10 + 5, ("Thread2-" + i).getBytes());
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        // 验证页面仍然有效
        assertTrue(page.isDirty());
        assertNotNull(page.getData());
    }
}
