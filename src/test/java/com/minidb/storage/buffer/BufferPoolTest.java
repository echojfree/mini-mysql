package com.minidb.storage.buffer;

import com.minidb.storage.file.TableSpace;
import com.minidb.storage.page.Page;
import com.minidb.storage.page.PageType;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 缓冲池测试
 *
 * 测试BufferPool、LRU替换算法
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BufferPoolTest {

    private static final String TEST_FILE = "test_buffer_pool.ibd";
    private static final int POOL_SIZE = 5;

    private TableSpace tableSpace;
    private BufferPool bufferPool;

    @BeforeEach
    void setUp() throws IOException {
        // 删除旧文件
        File file = new File(TEST_FILE);
        if (file.exists()) {
            file.delete();
        }

        // 创建表空间
        tableSpace = new TableSpace(1, TEST_FILE);

        // 准备测试数据:创建10个页面
        for (int i = 0; i < 10; i++) {
            Page page = tableSpace.allocatePage(PageType.INDEX);
            String data = "Page " + i + " data";
            page.writeData(0, data.getBytes());
            tableSpace.writePage(page);
        }
        tableSpace.flush();

        // 创建缓冲池
        bufferPool = new BufferPool(POOL_SIZE, tableSpace);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (tableSpace != null) {
            tableSpace.close();
        }

        // 清理文件
        File file = new File(TEST_FILE);
        if (file.exists()) {
            file.delete();
        }
    }

    /**
     * 测试缓存命中
     */
    @Test
    @Order(1)
    void testCacheHit() throws IOException {
        // 第一次读取 - Miss
        Page page0 = bufferPool.fetchPage(0);
        assertNotNull(page0);
        assertEquals(0, page0.getPageNo());
        assertEquals(0, bufferPool.getHitCount());
        assertEquals(1, bufferPool.getMissCount());

        bufferPool.unpinPage(0, false);

        // 第二次读取 - Hit
        Page page0Again = bufferPool.fetchPage(0);
        assertNotNull(page0Again);
        assertEquals(0, page0Again.getPageNo());
        assertEquals(1, bufferPool.getHitCount());
        assertEquals(1, bufferPool.getMissCount());
        assertEquals(0.5, bufferPool.getHitRate(), 0.001);

        bufferPool.unpinPage(0, false);

        System.out.println("\n=== Cache Hit Test ===");
        System.out.println(bufferPool.getStats());
    }

    /**
     * 测试LRU替换
     */
    @Test
    @Order(2)
    void testLRUReplacement() throws IOException {
        // 加载5个页面(填满缓冲池)
        for (int i = 0; i < POOL_SIZE; i++) {
            Page page = bufferPool.fetchPage(i);
            assertNotNull(page);
            bufferPool.unpinPage(i, false);
        }

        long missCount1 = bufferPool.getMissCount();
        assertEquals(POOL_SIZE, missCount1);

        // 访问页面1-4(使页面0成为最久未使用)
        for (int i = 1; i < POOL_SIZE; i++) {
            bufferPool.fetchPage(i);
            bufferPool.unpinPage(i, false);
        }

        // 应该都命中
        assertTrue(bufferPool.getHitCount() > 0);

        // 再读取一个新页面,应该触发LRU替换
        Page page5 = bufferPool.fetchPage(5);
        assertNotNull(page5);
        bufferPool.unpinPage(5, false);

        long missCount2 = bufferPool.getMissCount();
        assertTrue(missCount2 > missCount1); // 应该有新的miss

        System.out.println("\n=== LRU Replacement Test ===");
        System.out.println(bufferPool.getStats());
    }

    /**
     * 测试脏页刷新
     */
    @Test
    @Order(3)
    void testDirtyPageFlush() throws IOException {
        // 读取页面并修改
        Page page0 = bufferPool.fetchPage(0);
        page0.writeData(100, "Modified data".getBytes());
        bufferPool.unpinPage(0, true); // 标记为脏页

        // 刷新脏页
        bufferPool.flushPage(0);

        // 清空缓冲池并重新读取
        bufferPool = new BufferPool(POOL_SIZE, tableSpace);
        Page page0After = bufferPool.fetchPage(0);

        // 验证数据已持久化
        byte[] data = page0After.readData(100, "Modified data".length());
        String readData = new String(data);
        assertEquals("Modified data", readData);

        bufferPool.unpinPage(0, false);

        System.out.println("\n=== Dirty Page Flush Test ===");
        System.out.println("Data persisted successfully: " + readData);
    }

    /**
     * 测试Pin机制
     */
    @Test
    @Order(4)
    void testPinMechanism() throws IOException {
        // Pin页面0
        Page page0 = bufferPool.fetchPage(0);
        // 不unpin,保持pin状态

        // 尝试加载其他页面填满缓冲池
        for (int i = 1; i <= POOL_SIZE; i++) {
            Page page = bufferPool.fetchPage(i);
            bufferPool.unpinPage(i, false);
        }

        // 页面0应该仍然在缓冲池中(因为被pin)
        bufferPool.unpinPage(0, false);
        Page page0Again = bufferPool.fetchPage(0);
        assertNotNull(page0Again);
        assertEquals(0, page0Again.getPageNo());

        // 应该命中缓存
        assertTrue(bufferPool.getHitCount() > 0);

        bufferPool.unpinPage(0, false);

        System.out.println("\n=== Pin Mechanism Test ===");
        System.out.println(bufferPool.getStats());
    }

    /**
     * 测试缓存命中率
     */
    @Test
    @Order(5)
    void testHitRate() throws IOException {
        // 访问模式: 0,1,2,0,1,2,3,4
        int[] accessPattern = {0, 1, 2, 0, 1, 2, 3, 4};

        for (int pageNo : accessPattern) {
            Page page = bufferPool.fetchPage(pageNo);
            assertNotNull(page);
            bufferPool.unpinPage(pageNo, false);
        }

        // 期望: 前3次miss,后3次hit,最后2次miss
        // Hit: 3, Miss: 5, Rate: 37.5%
        assertEquals(3, bufferPool.getHitCount());
        assertEquals(5, bufferPool.getMissCount());
        assertEquals(0.375, bufferPool.getHitRate(), 0.001);

        System.out.println("\n=== Hit Rate Test ===");
        System.out.println("Access pattern: 0,1,2,0,1,2,3,4");
        System.out.println(bufferPool.getStats());
    }

    /**
     * 测试刷新所有脏页
     */
    @Test
    @Order(6)
    void testFlushAllPages() throws IOException {
        // 修改多个页面
        for (int i = 0; i < 3; i++) {
            Page page = bufferPool.fetchPage(i);
            page.writeData(0, ("Modified " + i).getBytes());
            bufferPool.unpinPage(i, true);
        }

        // 刷新所有脏页
        bufferPool.flushAllPages();

        // 验证: 重新加载后数据仍然存在
        bufferPool = new BufferPool(POOL_SIZE, tableSpace);

        for (int i = 0; i < 3; i++) {
            Page page = bufferPool.fetchPage(i);
            byte[] data = page.readData(0, ("Modified " + i).length());
            String readData = new String(data);
            assertEquals("Modified " + i, readData);
            bufferPool.unpinPage(i, false);
        }

        System.out.println("\n=== Flush All Pages Test ===");
        System.out.println("All dirty pages flushed successfully");
    }
}
