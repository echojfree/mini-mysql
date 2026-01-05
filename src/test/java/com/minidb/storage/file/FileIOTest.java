package com.minidb.storage.file;

import com.minidb.storage.page.Page;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;

import static com.minidb.common.Constants.PAGE_TYPE_DATA;
import static org.junit.jupiter.api.Assertions.*;

/**
 * TableSpace 和 DiskManager 的单元测试
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FileIOTest {

    private static final String TEST_DATA_DIR = "test_data";
    private static final String TEST_SPACE_NAME = "test_table";
    private static DiskManager diskManager;

    @BeforeAll
    static void setUp() {
        // 创建测试数据目录
        File testDir = new File(TEST_DATA_DIR);
        if (!testDir.exists()) {
            testDir.mkdirs();
        }

        // 初始化磁盘管理器
        diskManager = new DiskManager(TEST_DATA_DIR);
    }

    @AfterAll
    static void tearDown() throws IOException {
        // 关闭所有表空间
        if (diskManager != null) {
            diskManager.closeAll();
        }

        // 清理测试数据目录
        File testDir = new File(TEST_DATA_DIR);
        if (testDir.exists()) {
            deleteDirectory(testDir);
        }
    }

    /**
     * 测试创建表空间
     */
    @Test
    @Order(1)
    void testCreateTableSpace() throws IOException {
        int spaceId = diskManager.createTableSpace(TEST_SPACE_NAME);

        assertTrue(spaceId >= 0);
        assertEquals(spaceId, diskManager.getTableSpaceId(TEST_SPACE_NAME));

        // 验证文件是否创建
        File dataFile = new File(TEST_DATA_DIR + File.separator + TEST_SPACE_NAME + ".ibd");
        assertTrue(dataFile.exists());

        // 验证表空间已打开
        TableSpace tableSpace = diskManager.getTableSpace(spaceId);
        assertNotNull(tableSpace);
        assertTrue(tableSpace.isOpened());
        assertEquals(0, tableSpace.getPageCount());
    }

    /**
     * 测试分配页面
     */
    @Test
    @Order(2)
    void testAllocatePage() throws IOException {
        int spaceId = diskManager.getTableSpaceId(TEST_SPACE_NAME);
        assertTrue(spaceId >= 0);

        TableSpace tableSpace = diskManager.getTableSpace(spaceId);

        // 分配第一个页面
        int pageNumber1 = tableSpace.allocatePage();
        assertEquals(0, pageNumber1);
        assertEquals(1, tableSpace.getPageCount());

        // 分配第二个页面
        int pageNumber2 = tableSpace.allocatePage();
        assertEquals(1, pageNumber2);
        assertEquals(2, tableSpace.getPageCount());

        // 分配第三个页面
        int pageNumber3 = tableSpace.allocatePage();
        assertEquals(2, pageNumber3);
        assertEquals(3, tableSpace.getPageCount());
    }

    /**
     * 测试写入和读取页面
     */
    @Test
    @Order(3)
    void testWriteAndReadPage() throws IOException {
        int spaceId = diskManager.getTableSpaceId(TEST_SPACE_NAME);
        TableSpace tableSpace = diskManager.getTableSpace(spaceId);

        // 创建测试页面
        Page writePage = new Page(PAGE_TYPE_DATA, 0, spaceId);
        String testData = "Hello, Mini-MySQL! This is a test page.";
        writePage.writeData(0, testData.getBytes());
        writePage.updateChecksumAndLsn(1000L);

        // 写入磁盘
        tableSpace.writePage(writePage);

        // 从磁盘读取
        Page readPage = tableSpace.readPage(0);

        // 验证数据一致性
        assertNotNull(readPage);
        assertEquals(writePage.getHeader().getPageNumber(), readPage.getHeader().getPageNumber());
        assertEquals(writePage.getHeader().getPageType(), readPage.getHeader().getPageType());
        assertEquals(writePage.getHeader().getSpaceId(), readPage.getHeader().getSpaceId());
        assertEquals(writePage.getHeader().getLastModifyLsn(), readPage.getHeader().getLastModifyLsn());

        // 验证数据内容
        byte[] readData = readPage.readData(0, testData.getBytes().length);
        assertArrayEquals(testData.getBytes(), readData);
        assertEquals(testData, new String(readData));
    }

    /**
     * 测试写入多个页面
     */
    @Test
    @Order(4)
    void testWriteMultiplePages() throws IOException {
        int spaceId = diskManager.getTableSpaceId(TEST_SPACE_NAME);
        TableSpace tableSpace = diskManager.getTableSpace(spaceId);

        // 写入 10 个页面
        for (int i = 0; i < 10; i++) {
            Page page = new Page(PAGE_TYPE_DATA, i, spaceId);
            String data = "Page " + i + " - Test Data";
            page.writeData(0, data.getBytes());
            page.updateChecksumAndLsn(2000L + i);
            tableSpace.writePage(page);
        }

        // 读取并验证所有页面
        for (int i = 0; i < 10; i++) {
            Page page = tableSpace.readPage(i);
            assertNotNull(page);
            assertEquals(i, page.getHeader().getPageNumber());

            String expectedData = "Page " + i + " - Test Data";
            byte[] readData = page.readData(0, expectedData.getBytes().length);
            assertEquals(expectedData, new String(readData));
        }
    }

    /**
     * 测试页面完整性验证
     */
    @Test
    @Order(5)
    void testPageIntegrityCheck() throws IOException {
        int spaceId = diskManager.getTableSpaceId(TEST_SPACE_NAME);
        TableSpace tableSpace = diskManager.getTableSpace(spaceId);

        // 创建并写入页面
        Page writePage = new Page(PAGE_TYPE_DATA, 15, spaceId);
        writePage.writeData(0, "Integrity Test".getBytes());
        writePage.updateChecksumAndLsn(3000L);
        tableSpace.writePage(writePage);

        // 读取页面并验证完整性
        Page readPage = tableSpace.readPage(15);
        assertTrue(readPage.verify());
    }

    /**
     * 测试 DiskManager 的页面读写接口
     */
    @Test
    @Order(6)
    void testDiskManagerReadWrite() throws IOException {
        int spaceId = diskManager.getTableSpaceId(TEST_SPACE_NAME);

        // 分配新页面
        int pageNumber = diskManager.allocatePage(spaceId);
        assertTrue(pageNumber >= 0);

        // 创建页面并写入
        Page writePage = new Page(PAGE_TYPE_DATA, pageNumber, spaceId);
        String testData = "DiskManager Test";
        writePage.writeData(0, testData.getBytes());
        writePage.updateChecksumAndLsn(4000L);

        diskManager.writePage(spaceId, writePage);

        // 读取并验证
        Page readPage = diskManager.readPage(spaceId, pageNumber);
        assertNotNull(readPage);

        byte[] readData = readPage.readData(0, testData.getBytes().length);
        assertEquals(testData, new String(readData));
    }

    /**
     * 测试同步操作
     */
    @Test
    @Order(7)
    void testSync() throws IOException {
        int spaceId = diskManager.getTableSpaceId(TEST_SPACE_NAME);
        TableSpace tableSpace = diskManager.getTableSpace(spaceId);

        // 创建并写入页面
        Page page = new Page(PAGE_TYPE_DATA, 20, spaceId);
        page.writeData(0, "Sync Test".getBytes());
        page.updateChecksumAndLsn(5000L);
        tableSpace.writePage(page);

        // 同步到磁盘
        assertDoesNotThrow(() -> tableSpace.sync());
        assertDoesNotThrow(() -> diskManager.syncAll());
    }

    /**
     * 测试无效页号的读取
     */
    @Test
    @Order(8)
    void testReadInvalidPage() {
        int spaceId = diskManager.getTableSpaceId(TEST_SPACE_NAME);
        TableSpace tableSpace = diskManager.getTableSpace(spaceId);

        // 尝试读取不存在的页面
        assertThrows(IllegalArgumentException.class, () -> {
            tableSpace.readPage(999999);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            tableSpace.readPage(-1);
        });
    }

    /**
     * 测试 getOrCreateTableSpace
     */
    @Test
    @Order(9)
    void testGetOrCreateTableSpace() throws IOException {
        String newSpaceName = "test_get_or_create";

        // 第一次调用应该创建新表空间
        int spaceId1 = diskManager.getOrCreateTableSpace(newSpaceName);
        assertTrue(spaceId1 >= 0);

        // 第二次调用应该返回相同的 ID
        int spaceId2 = diskManager.getOrCreateTableSpace(newSpaceName);
        assertEquals(spaceId1, spaceId2);

        // 验证文件存在
        File dataFile = new File(TEST_DATA_DIR + File.separator + newSpaceName + ".ibd");
        assertTrue(dataFile.exists());
    }

    /**
     * 测试关闭表空间
     */
    @Test
    @Order(10)
    void testCloseTableSpace() throws IOException {
        String spaceName = "test_close";
        int spaceId = diskManager.createTableSpace(spaceName);

        TableSpace tableSpace = diskManager.getTableSpace(spaceId);
        assertTrue(tableSpace.isOpened());

        // 关闭表空间
        diskManager.closeTableSpace(spaceId);

        // 验证已关闭
        assertThrows(IllegalArgumentException.class, () -> {
            diskManager.getTableSpace(spaceId);
        });

        assertEquals(-1, diskManager.getTableSpaceId(spaceName));
    }

    /**
     * 测试删除表空间
     */
    @Test
    @Order(11)
    void testDeleteTableSpace() throws IOException {
        String spaceName = "test_delete";
        int spaceId = diskManager.createTableSpace(spaceName);

        File dataFile = new File(TEST_DATA_DIR + File.separator + spaceName + ".ibd");
        assertTrue(dataFile.exists());

        // 删除表空间
        boolean deleted = diskManager.deleteTableSpace(spaceId);
        assertTrue(deleted);

        // 验证文件已删除
        assertFalse(dataFile.exists());

        // 验证从映射表中移除
        assertEquals(-1, diskManager.getTableSpaceId(spaceName));
    }

    /**
     * 测试大量数据写入和读取
     */
    @Test
    @Order(12)
    void testLargeDataWriteAndRead() throws IOException {
        String spaceName = "test_large_data";
        int spaceId = diskManager.createTableSpace(spaceName);

        // 写入 100 个页面
        int pageCount = 100;
        for (int i = 0; i < pageCount; i++) {
            int pageNumber = diskManager.allocatePage(spaceId);
            Page page = new Page(PAGE_TYPE_DATA, pageNumber, spaceId);

            // 写入大量数据
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < 100; j++) {
                sb.append("Page ").append(i).append(" Line ").append(j).append(" | ");
            }
            byte[] data = sb.toString().getBytes();

            page.writeData(0, data);
            page.updateChecksumAndLsn(6000L + i);
            diskManager.writePage(spaceId, page);
        }

        // 验证所有页面
        TableSpace tableSpace = diskManager.getTableSpace(spaceId);
        assertEquals(pageCount, tableSpace.getPageCount());

        // 随机读取一些页面验证
        for (int i = 0; i < 10; i++) {
            int randomPage = (int) (Math.random() * pageCount);
            Page page = diskManager.readPage(spaceId, randomPage);
            assertNotNull(page);
            assertTrue(page.verify());
        }
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
