package com.minidb.storage;

import com.minidb.storage.file.TableSpace;
import com.minidb.storage.page.Page;
import com.minidb.storage.page.PageType;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 存储引擎测试
 *
 * 测试 Page、TableSpace 的功能
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StorageEngineTest {

    private static final String TEST_FILE = "test_tablespace.ibd";
    private TableSpace tableSpace;

    @BeforeEach
    void setUp() throws IOException {
        // 删除旧文件
        File file = new File(TEST_FILE);
        if (file.exists()) {
            file.delete();
        }

        // 创建表空间
        tableSpace = new TableSpace(1, TEST_FILE);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (tableSpace != null) {
            tableSpace.close();
        }

        // 清理测试文件
        File file = new File(TEST_FILE);
        if (file.exists()) {
            file.delete();
        }
    }

    /**
     * 测试创建页面
     */
    @Test
    @Order(1)
    void testCreatePage() {
        Page page = new Page(1, 0, PageType.INDEX);

        assertNotNull(page);
        assertEquals(0, page.getPageNo());
        assertEquals(PageType.INDEX, page.getPageType());
        assertEquals(Page.PAGE_SIZE, 16 * 1024);

        System.out.println("\n=== Create Page Test ===");
        System.out.println(page);
    }

    /**
     * 测试页面读写数据
     */
    @Test
    @Order(2)
    void testPageReadWrite() {
        Page page = new Page(1, 0, PageType.INDEX);

        // 写入数据
        String testData = "Hello, Mini-MySQL!";
        byte[] bytes = testData.getBytes();
        page.writeData(0, bytes);

        // 读取数据
        byte[] readBytes = page.readData(0, bytes.length);
        String readData = new String(readBytes);

        assertEquals(testData, readData);
        assertTrue(page.isDirty());

        System.out.println("\n=== Page Read/Write Test ===");
        System.out.println("Written: " + testData);
        System.out.println("Read: " + readData);
        System.out.println("Page is dirty: " + page.isDirty());
    }

    /**
     * 测试页面序列化和反序列化
     */
    @Test
    @Order(3)
    void testPageSerialization() {
        // 创建页面并写入数据
        Page originalPage = new Page(1, 5, PageType.INDEX);
        String testData = "Test serialization data";
        originalPage.writeData(100, testData.getBytes());

        // 序列化
        byte[] serialized = originalPage.serialize();
        assertEquals(Page.PAGE_SIZE, serialized.length);

        // 反序列化
        Page deserializedPage = Page.deserialize(serialized);

        // 验证
        assertEquals(originalPage.getPageNo(), deserializedPage.getPageNo());
        assertEquals(originalPage.getPageType(), deserializedPage.getPageType());

        // 验证数据
        byte[] readBytes = deserializedPage.readData(100, testData.getBytes().length);
        String readData = new String(readBytes);
        assertEquals(testData, readData);

        System.out.println("\n=== Page Serialization Test ===");
        System.out.println("Original: " + originalPage);
        System.out.println("Deserialized: " + deserializedPage);
        System.out.println("Data match: " + testData.equals(readData));
    }

    /**
     * 测试分配页面
     */
    @Test
    @Order(4)
    void testAllocatePage() {
        Page page1 = tableSpace.allocatePage(PageType.INDEX);
        Page page2 = tableSpace.allocatePage(PageType.INDEX);
        Page page3 = tableSpace.allocatePage(PageType.UNDO_LOG);

        assertEquals(0, page1.getPageNo());
        assertEquals(1, page2.getPageNo());
        assertEquals(2, page3.getPageNo());
        assertEquals(3, tableSpace.getPageCount());

        System.out.println("\n=== Allocate Page Test ===");
        System.out.println("Page 1: " + page1);
        System.out.println("Page 2: " + page2);
        System.out.println("Page 3: " + page3);
        System.out.println("Total pages: " + tableSpace.getPageCount());
    }

    /**
     * 测试写入和读取页面
     */
    @Test
    @Order(5)
    void testWriteAndReadPage() throws IOException {
        // 分配页面
        Page page = tableSpace.allocatePage(PageType.INDEX);
        assertEquals(0, page.getPageNo());

        // 写入数据
        String testData = "Persistent data test";
        page.writeData(50, testData.getBytes());

        // 写入文件
        tableSpace.writePage(page);
        tableSpace.flush();

        // 读取页面
        Page readPage = tableSpace.readPage(0);

        // 验证
        assertEquals(page.getPageNo(), readPage.getPageNo());
        assertEquals(page.getPageType(), readPage.getPageType());

        // 验证数据
        byte[] readBytes = readPage.readData(50, testData.getBytes().length);
        String readData = new String(readBytes);
        assertEquals(testData, readData);

        System.out.println("\n=== Write and Read Page Test ===");
        System.out.println("Written page: " + page);
        System.out.println("Read page: " + readPage);
        System.out.println("Data match: " + testData.equals(readData));
    }

    /**
     * 测试持久化和恢复
     */
    @Test
    @Order(6)
    void testPersistenceAndRecovery() throws IOException {
        // 第一阶段: 写入数据
        {
            Page page1 = tableSpace.allocatePage(PageType.INDEX);
            Page page2 = tableSpace.allocatePage(PageType.INDEX);

            page1.writeData(0, "Page 1 data".getBytes());
            page2.writeData(0, "Page 2 data".getBytes());

            tableSpace.writePage(page1);
            tableSpace.writePage(page2);
            tableSpace.flush();
            tableSpace.close();

            System.out.println("\n=== Persistence Test - Phase 1: Write ===");
            System.out.println("Written 2 pages to file");
        }

        // 第二阶段: 重新打开并读取
        {
            tableSpace = new TableSpace(1, TEST_FILE);

            assertEquals(2, tableSpace.getPageCount());

            Page page1 = tableSpace.readPage(0);
            Page page2 = tableSpace.readPage(1);

            String data1 = new String(page1.readData(0, 11));
            String data2 = new String(page2.readData(0, 11));

            assertEquals("Page 1 data", data1);
            assertEquals("Page 2 data", data2);

            System.out.println("\n=== Persistence Test - Phase 2: Recovery ===");
            System.out.println("Recovered page 1: " + data1);
            System.out.println("Recovered page 2: " + data2);
            System.out.println("Total pages: " + tableSpace.getPageCount());
        }
    }

    /**
     * 测试多页写入
     */
    @Test
    @Order(7)
    void testMultiplePages() throws IOException {
        final int PAGE_COUNT = 10;

        // 写入多个页面
        for (int i = 0; i < PAGE_COUNT; i++) {
            Page page = tableSpace.allocatePage(PageType.INDEX);
            String data = "Page " + i + " content";
            page.writeData(0, data.getBytes());
            tableSpace.writePage(page);
        }

        tableSpace.flush();

        // 验证
        assertEquals(PAGE_COUNT, tableSpace.getPageCount());

        // 随机读取验证
        Page page5 = tableSpace.readPage(5);
        byte[] data5Bytes = page5.readData(0, "Page 5 content".length());
        String data5 = new String(data5Bytes);
        assertEquals("Page 5 content", data5);

        System.out.println("\n=== Multiple Pages Test ===");
        System.out.println("Written " + PAGE_COUNT + " pages");
        System.out.println("File size: " + tableSpace.getFileSize() + " bytes");
        System.out.println("Expected: " + (PAGE_COUNT * Page.PAGE_SIZE) + " bytes");
        System.out.println("Sample read (page 5): " + data5);
    }

    /**
     * 测试页面大小限制
     */
    @Test
    @Order(8)
    void testPageSizeLimit() {
        Page page = new Page(1, 0, PageType.INDEX);

        // 尝试写入超出范围的数据
        byte[] largeData = new byte[Page.USER_DATA_SIZE + 1];

        assertThrows(IllegalArgumentException.class, () -> {
            page.writeData(0, largeData);
        });

        // 尝试在无效偏移量写入
        assertThrows(IllegalArgumentException.class, () -> {
            page.writeData(Page.USER_DATA_SIZE, new byte[1]);
        });

        System.out.println("\n=== Page Size Limit Test ===");
        System.out.println("Page size: " + Page.PAGE_SIZE);
        System.out.println("User data size: " + Page.USER_DATA_SIZE);
        System.out.println("Correctly rejected oversized write");
    }
}
