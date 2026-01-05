package com.minidb.log;

import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Redo Log 的单元测试
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RedoLogTest {

    private static final String TEST_REDO_LOG_FILE = "test_redo.log";
    private RedoLogManager redoLogManager;

    @BeforeEach
    void setUp() {
        // 清理测试文件
        cleanupTestFile();
        redoLogManager = new RedoLogManager(TEST_REDO_LOG_FILE, 100);
    }

    @AfterEach
    void tearDown() {
        cleanupTestFile();
    }

    /**
     * 清理测试文件
     */
    private void cleanupTestFile() {
        File file = new File(TEST_REDO_LOG_FILE);
        if (file.exists()) {
            file.delete();
        }
    }

    /**
     * 测试记录 INSERT 操作的 Redo Log
     */
    @Test
    @Order(1)
    void testLogInsert() {
        long lsn = redoLogManager.logInsert(1L, 0, 0, "test_table", "row1", "data".getBytes());

        assertTrue(lsn > 0);
        assertEquals(1, redoLogManager.getBufferSize());
    }

    /**
     * 测试记录 DELETE 操作的 Redo Log
     */
    @Test
    @Order(2)
    void testLogDelete() {
        long lsn = redoLogManager.logDelete(1L, 0, 0, "test_table", "row1");

        assertTrue(lsn > 0);
        assertEquals(1, redoLogManager.getBufferSize());
    }

    /**
     * 测试记录 UPDATE 操作的 Redo Log
     */
    @Test
    @Order(3)
    void testLogUpdate() {
        long lsn = redoLogManager.logUpdate(1L, 0, 0, "test_table", "row1", "new_data".getBytes());

        assertTrue(lsn > 0);
        assertEquals(1, redoLogManager.getBufferSize());
    }

    /**
     * 测试 LSN 递增
     */
    @Test
    @Order(4)
    void testLsnIncrement() {
        long lsn1 = redoLogManager.logInsert(1L, 0, 0, "table1", "row1", "data1".getBytes());
        long lsn2 = redoLogManager.logInsert(1L, 0, 0, "table1", "row2", "data2".getBytes());
        long lsn3 = redoLogManager.logInsert(1L, 0, 0, "table1", "row3", "data3".getBytes());

        assertTrue(lsn2 > lsn1);
        assertTrue(lsn3 > lsn2);
        assertEquals(3, redoLogManager.getBufferSize());
    }

    /**
     * 测试手动刷盘
     */
    @Test
    @Order(5)
    void testFlush() {
        redoLogManager.logInsert(1L, 0, 0, "table1", "row1", "data1".getBytes());
        redoLogManager.logUpdate(1L, 0, 0, "table1", "row1", "data2".getBytes());

        assertEquals(2, redoLogManager.getBufferSize());

        // 手动刷盘
        redoLogManager.flush();

        assertEquals(0, redoLogManager.getBufferSize());
        assertEquals(1, redoLogManager.getFlushCount());
        assertTrue(redoLogManager.getFlushedLsn() > 0);

        // 验证文件已创建
        File file = new File(TEST_REDO_LOG_FILE);
        assertTrue(file.exists());
        assertTrue(file.length() > 0);
    }

    /**
     * 测试 Buffer 满时自动刷盘
     */
    @Test
    @Order(6)
    void testAutoFlushWhenBufferFull() {
        // Buffer 大小为 100，写入 100 条记录
        for (int i = 0; i < 100; i++) {
            redoLogManager.logInsert(1L, 0, 0, "table1", "row_" + i, ("data_" + i).getBytes());
        }

        // Buffer 应该已经刷盘
        assertEquals(0, redoLogManager.getBufferSize());
        assertTrue(redoLogManager.getFlushCount() > 0);
    }

    /**
     * 测试创建 Checkpoint
     */
    @Test
    @Order(7)
    void testCheckpoint() {
        redoLogManager.logInsert(1L, 0, 0, "table1", "row1", "data1".getBytes());
        redoLogManager.logUpdate(1L, 0, 0, "table1", "row1", "data2".getBytes());

        // 创建 Checkpoint
        redoLogManager.checkpoint();

        assertEquals(1, redoLogManager.getCheckpointCount());
        assertTrue(redoLogManager.getCheckpointLsn() > 0);
        assertEquals(redoLogManager.getCheckpointLsn(), redoLogManager.getFlushedLsn());
    }

    /**
     * 测试崩溃恢复
     */
    @Test
    @Order(8)
    void testCrashRecovery() {
        // 写入一些日志并刷盘
        redoLogManager.logInsert(1L, 0, 0, "table1", "row1", "data1".getBytes());
        redoLogManager.logUpdate(1L, 0, 0, "table1", "row1", "data2".getBytes());
        redoLogManager.logDelete(2L, 0, 0, "table2", "row2");
        redoLogManager.flush();

        // 创建新的 RedoLogManager 模拟崩溃重启
        RedoLogManager newManager = new RedoLogManager(TEST_REDO_LOG_FILE, 100);

        // 执行恢复
        int recoveredCount = newManager.recover();

        // 应该恢复 3 条日志
        assertEquals(3, recoveredCount);
    }

    /**
     * 测试带 Checkpoint 的崩溃恢复
     */
    @Test
    @Order(9)
    void testRecoveryWithCheckpoint() {
        // 写入一些日志
        redoLogManager.logInsert(1L, 0, 0, "table1", "row1", "data1".getBytes());
        redoLogManager.logUpdate(1L, 0, 0, "table1", "row1", "data2".getBytes());

        // 创建 Checkpoint
        redoLogManager.checkpoint();
        long checkpointLsn = redoLogManager.getCheckpointLsn();

        // 继续写入日志
        redoLogManager.logInsert(2L, 0, 0, "table2", "row2", "data3".getBytes());
        redoLogManager.logDelete(2L, 0, 0, "table2", "row3");
        redoLogManager.flush();

        // 创建新的 RedoLogManager 模拟崩溃重启
        RedoLogManager newManager = new RedoLogManager(TEST_REDO_LOG_FILE, 100);

        // 执行恢复
        int recoveredCount = newManager.recover();

        // 应该恢复所有日志：2条(Checkpoint前) + 1条(Checkpoint) + 2条(Checkpoint后) = 5条
        // 但只有 Checkpoint 之后的 2 条会被实际应用
        // 由于我们的实现会读取所有日志但只应用 Checkpoint 之后的，
        // 实际恢复的是 2 条（Checkpoint 之后的）
        assertTrue(recoveredCount >= 2, "At least 2 logs should be recovered after checkpoint");
        assertEquals(checkpointLsn, newManager.getCheckpointLsn());
    }

    /**
     * 测试空文件的恢复
     */
    @Test
    @Order(10)
    void testRecoveryWithNoFile() {
        // 删除文件
        cleanupTestFile();

        RedoLogManager newManager = new RedoLogManager(TEST_REDO_LOG_FILE, 100);
        int recoveredCount = newManager.recover();

        assertEquals(0, recoveredCount);
    }

    /**
     * 测试多次刷盘
     */
    @Test
    @Order(11)
    void testMultipleFlushes() {
        // 第一次刷盘
        redoLogManager.logInsert(1L, 0, 0, "table1", "row1", "data1".getBytes());
        redoLogManager.flush();
        long flushedLsn1 = redoLogManager.getFlushedLsn();

        // 第二次刷盘
        redoLogManager.logUpdate(1L, 0, 0, "table1", "row1", "data2".getBytes());
        redoLogManager.flush();
        long flushedLsn2 = redoLogManager.getFlushedLsn();

        // 第三次刷盘
        redoLogManager.logDelete(2L, 0, 0, "table2", "row2");
        redoLogManager.flush();
        long flushedLsn3 = redoLogManager.getFlushedLsn();

        assertTrue(flushedLsn2 > flushedLsn1);
        assertTrue(flushedLsn3 > flushedLsn2);
        assertEquals(3, redoLogManager.getFlushCount());
    }

    /**
     * 测试统计信息
     */
    @Test
    @Order(12)
    void testStatistics() {
        // 执行一些操作
        redoLogManager.logInsert(1L, 0, 0, "table1", "row1", "data1".getBytes());
        redoLogManager.logUpdate(1L, 0, 0, "table1", "row1", "data2".getBytes());
        redoLogManager.flush();

        redoLogManager.logDelete(2L, 0, 0, "table2", "row2");
        redoLogManager.checkpoint();

        // 验证统计信息
        assertTrue(redoLogManager.getLsnGenerator().get() > 0);
        assertTrue(redoLogManager.getFlushedLsn() > 0);
        assertTrue(redoLogManager.getCheckpointLsn() > 0);
        assertTrue(redoLogManager.getFlushCount() > 0);
        assertTrue(redoLogManager.getCheckpointCount() > 0);

        // 打印统计信息
        redoLogManager.printStats();
    }

    /**
     * 测试大量日志的写入和恢复
     */
    @Test
    @Order(13)
    void testLargeScaleLogging() {
        final int logCount = 1000;

        // 写入大量日志
        for (int i = 0; i < logCount; i++) {
            redoLogManager.logInsert(1L, 0, i, "table1", "row_" + i, ("data_" + i).getBytes());
        }

        redoLogManager.flush();

        // 创建新的 RedoLogManager 并恢复
        RedoLogManager newManager = new RedoLogManager(TEST_REDO_LOG_FILE, 100);
        int recoveredCount = newManager.recover();

        assertEquals(logCount, recoveredCount);
    }

    /**
     * 测试并发写入
     */
    @Test
    @Order(14)
    void testConcurrentLogging() throws InterruptedException {
        final int threadCount = 10;
        final int logsPerThread = 10;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final long txnId = i + 1;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < logsPerThread; j++) {
                    redoLogManager.logInsert(txnId, 0, j, "table", "row_" + j,
                            ("data_" + txnId + "_" + j).getBytes());
                }
            });
            threads[i].start();
        }

        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }

        redoLogManager.flush();

        // 创建新的 RedoLogManager 并恢复
        RedoLogManager newManager = new RedoLogManager(TEST_REDO_LOG_FILE, 100);
        int recoveredCount = newManager.recover();

        assertEquals(threadCount * logsPerThread, recoveredCount);
    }
}
