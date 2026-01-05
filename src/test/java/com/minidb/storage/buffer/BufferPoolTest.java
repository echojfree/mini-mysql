package com.minidb.storage.buffer;

import com.minidb.storage.file.DiskManager;
import com.minidb.storage.page.Page;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;

import static com.minidb.common.Constants.PAGE_TYPE_DATA;
import static org.junit.jupiter.api.Assertions.*;

/**
 * BufferPool 的单元测试
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BufferPoolTest {

    private static final String TEST_DATA_DIR = "test_buffer_pool";
    private static final String TEST_SPACE_NAME = "test_buffer";
    private static DiskManager diskManager;
    private static int testSpaceId;
    private BufferPool bufferPool;

    @BeforeAll
    static void setUpAll() throws IOException {
        // 创建测试数据目录
        File testDir = new File(TEST_DATA_DIR);
        if (!testDir.exists()) {
            testDir.mkdirs();
        }

        // 初始化磁盘管理器
        diskManager = new DiskManager(TEST_DATA_DIR);
        testSpaceId = diskManager.createTableSpace(TEST_SPACE_NAME);

        // 预先创建一些页面到磁盘
        for (int i = 0; i < 20; i++) {
            int pageNumber = diskManager.allocatePage(testSpaceId);
            Page page = new Page(PAGE_TYPE_DATA, pageNumber, testSpaceId);
            String data = "Page_" + i + "_Data";
            page.writeData(0, data.getBytes());
            page.updateChecksumAndLsn(1000L + i);
            diskManager.writePage(testSpaceId, page);
        }
    }

    @AfterAll
    static void tearDownAll() throws IOException {
        if (diskManager != null) {
            diskManager.closeAll();
        }

        // 清理测试数据目录
        File testDir = new File(TEST_DATA_DIR);
        if (testDir.exists()) {
            deleteDirectory(testDir);
        }
    }

    @BeforeEach
    void setUp() {
        // 每个测试使用新的缓冲池
        bufferPool = new BufferPool(10, diskManager);
    }

    /**
     * 测试创建缓冲池
     */
    @Test
    @Order(1)
    void testCreateBufferPool() {
        assertNotNull(bufferPool);
        assertEquals(10, bufferPool.getPoolSize());
        assertEquals(0, bufferPool.getUsedFrameCount());
        assertEquals(0, bufferPool.getDirtyPageCount());
        assertEquals(0.0, bufferPool.getHitRate());
    }

    /**
     * 测试获取页面（第一次 - MISS）
     */
    @Test
    @Order(2)
    void testFetchPageMiss() throws IOException {
        Page page = bufferPool.fetchPage(testSpaceId, 0);

        assertNotNull(page);
        assertEquals(0, page.getHeader().getPageNumber());
        assertEquals(testSpaceId, page.getHeader().getSpaceId());
        assertEquals(1, bufferPool.getUsedFrameCount());
        assertEquals(0.0, bufferPool.getHitRate()); // 第一次是 MISS
    }

    /**
     * 测试获取页面（第二次 - HIT）
     */
    @Test
    @Order(3)
    void testFetchPageHit() throws IOException {
        // 第一次获取
        Page page1 = bufferPool.fetchPage(testSpaceId, 0);
        assertNotNull(page1);

        // 第二次获取（应该命中缓存）
        Page page2 = bufferPool.fetchPage(testSpaceId, 0);
        assertNotNull(page2);
        assertSame(page1, page2); // 应该是同一个对象

        assertEquals(1, bufferPool.getUsedFrameCount());
        assertEquals(0.5, bufferPool.getHitRate()); // 1 HIT / 2 requests = 50%
    }

    /**
     * 测试 unpin 页面
     */
    @Test
    @Order(4)
    void testUnpinPage() throws IOException {
        Page page = bufferPool.fetchPage(testSpaceId, 0);
        assertNotNull(page);

        // unpin 页面
        bufferPool.unpinPage(testSpaceId, 0, false);

        // 页面仍然在缓冲池中
        assertEquals(1, bufferPool.getUsedFrameCount());
    }

    /**
     * 测试标记脏页
     */
    @Test
    @Order(5)
    void testDirtyPage() throws IOException {
        Page page = bufferPool.fetchPage(testSpaceId, 0);
        assertNotNull(page);

        // 修改页面内容
        page.writeData(0, "Modified Data".getBytes());

        // unpin 并标记为脏页
        bufferPool.unpinPage(testSpaceId, 0, true);

        assertEquals(1, bufferPool.getDirtyPageCount());
    }

    /**
     * 测试刷盘单个页面
     */
    @Test
    @Order(6)
    void testFlushPage() throws IOException {
        Page page = bufferPool.fetchPage(testSpaceId, 0);
        page.writeData(0, "Flush Test".getBytes());
        bufferPool.unpinPage(testSpaceId, 0, true);

        assertEquals(1, bufferPool.getDirtyPageCount());

        // 刷盘
        bufferPool.flushPage(testSpaceId, 0);

        assertEquals(0, bufferPool.getDirtyPageCount());

        // 验证数据已写入磁盘
        Page diskPage = diskManager.readPage(testSpaceId, 0);
        byte[] data = diskPage.readData(0, "Flush Test".getBytes().length);
        assertEquals("Flush Test", new String(data));
    }

    /**
     * 测试刷盘所有页面
     */
    @Test
    @Order(7)
    void testFlushAllPages() throws IOException {
        // 获取并修改多个页面
        for (int i = 0; i < 5; i++) {
            Page page = bufferPool.fetchPage(testSpaceId, i);
            page.writeData(0, ("Data_" + i).getBytes());
            bufferPool.unpinPage(testSpaceId, i, true);
        }

        assertEquals(5, bufferPool.getDirtyPageCount());

        // 刷盘所有页面
        bufferPool.flushAllPages();

        assertEquals(0, bufferPool.getDirtyPageCount());
    }

    /**
     * 测试缓冲池填满
     */
    @Test
    @Order(8)
    void testBufferPoolFull() throws IOException {
        // 缓冲池大小为 10，填满它
        for (int i = 0; i < 10; i++) {
            Page page = bufferPool.fetchPage(testSpaceId, i);
            assertNotNull(page);
            bufferPool.unpinPage(testSpaceId, i, false);
        }

        assertEquals(10, bufferPool.getUsedFrameCount());
    }

    /**
     * 测试 LRU 驱逐
     */
    @Test
    @Order(9)
    void testLRUEviction() throws IOException {
        // 填满缓冲池（大小为 10）
        for (int i = 0; i < 10; i++) {
            Page page = bufferPool.fetchPage(testSpaceId, i);
            bufferPool.unpinPage(testSpaceId, i, false);
        }

        assertEquals(10, bufferPool.getUsedFrameCount());
        long evictionsBefore = bufferPool.getEvictionCount();

        // 再获取一个新页面，应该触发驱逐
        Page newPage = bufferPool.fetchPage(testSpaceId, 10);
        assertNotNull(newPage);
        bufferPool.unpinPage(testSpaceId, 10, false);

        // 缓冲池仍然是满的
        assertEquals(10, bufferPool.getUsedFrameCount());

        // 应该发生了一次驱逐
        assertEquals(evictionsBefore + 1, bufferPool.getEvictionCount());
    }

    /**
     * 测试驱逐脏页时自动刷盘
     */
    @Test
    @Order(10)
    void testEvictDirtyPage() throws IOException {
        // 填满缓冲池并标记为脏页
        for (int i = 0; i < 10; i++) {
            Page page = bufferPool.fetchPage(testSpaceId, i);
            page.writeData(0, ("Dirty_" + i).getBytes());
            bufferPool.unpinPage(testSpaceId, i, true);
        }

        assertEquals(10, bufferPool.getDirtyPageCount());

        // 获取新页面，触发驱逐（脏页应该自动刷盘）
        Page newPage = bufferPool.fetchPage(testSpaceId, 10);
        assertNotNull(newPage);

        // 脏页数量应该减少（被驱逐的脏页已刷盘）
        assertTrue(bufferPool.getDirtyPageCount() < 10);
    }

    /**
     * 测试高命中率场景
     */
    @Test
    @Order(11)
    void testHighHitRate() throws IOException {
        // 加载 5 个页面
        for (int i = 0; i < 5; i++) {
            bufferPool.fetchPage(testSpaceId, i);
            bufferPool.unpinPage(testSpaceId, i, false);
        }

        // 重复访问这 5 个页面
        for (int round = 0; round < 10; round++) {
            for (int i = 0; i < 5; i++) {
                bufferPool.fetchPage(testSpaceId, i);
                bufferPool.unpinPage(testSpaceId, i, false);
            }
        }

        // 命中率应该很高
        // 5 MISS + 50 HIT = 55 requests
        // Hit rate = 50 / 55 ≈ 90.9%
        assertTrue(bufferPool.getHitRate() > 0.90);
    }

    /**
     * 测试并发 pin/unpin
     */
    @Test
    @Order(12)
    void testConcurrentPinUnpin() throws IOException {
        Page page = bufferPool.fetchPage(testSpaceId, 0);

        // pin 多次
        bufferPool.fetchPage(testSpaceId, 0);
        bufferPool.fetchPage(testSpaceId, 0);

        // unpin 多次
        bufferPool.unpinPage(testSpaceId, 0, false);
        bufferPool.unpinPage(testSpaceId, 0, false);
        bufferPool.unpinPage(testSpaceId, 0, false);

        // 页面仍然在缓冲池中
        assertEquals(1, bufferPool.getUsedFrameCount());
    }

    /**
     * 测试统计信息
     */
    @Test
    @Order(13)
    void testStatistics() throws IOException {
        // 执行一些操作
        for (int i = 0; i < 5; i++) {
            bufferPool.fetchPage(testSpaceId, i);
            bufferPool.unpinPage(testSpaceId, i, i % 2 == 0); // 偶数页标记为脏页
        }

        // 验证统计信息
        assertTrue(bufferPool.getRequestCount() > 0);
        assertTrue(bufferPool.getHitCount() >= 0);
        assertEquals(3, bufferPool.getDirtyPageCount()); // 0, 2, 4
        assertEquals(5, bufferPool.getUsedFrameCount());

        // 打印统计信息（用于手动验证）
        bufferPool.printStats();
    }

    /**
     * 测试大量页面访问
     */
    @Test
    @Order(14)
    void testLargeScale() throws IOException {
        int pageCount = 20; // 大于缓冲池大小（10）

        // 顺序访问所有页面
        for (int i = 0; i < pageCount; i++) {
            Page page = bufferPool.fetchPage(testSpaceId, i);
            assertNotNull(page);
            bufferPool.unpinPage(testSpaceId, i, false);
        }

        // 缓冲池应该是满的
        assertEquals(10, bufferPool.getUsedFrameCount());

        // 应该发生了驱逐
        assertTrue(bufferPool.getEvictionCount() > 0);

        bufferPool.printStats();
    }

    /**
     * 测试异常情况：unpin 不存在的页面
     */
    @Test
    @Order(15)
    void testUnpinNonExistentPage() {
        // 不应该抛出异常
        assertDoesNotThrow(() -> {
            bufferPool.unpinPage(testSpaceId, 999, false);
        });
    }

    /**
     * 测试异常情况：flush 不存在的页面
     */
    @Test
    @Order(16)
    void testFlushNonExistentPage() {
        // 不应该抛出异常
        assertDoesNotThrow(() -> {
            bufferPool.flushPage(testSpaceId, 999);
        });
    }

    /**
     * 递归删除目录
     */
    private static void deleteDirectory(File directory) {
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        directory.delete();
    }
}
