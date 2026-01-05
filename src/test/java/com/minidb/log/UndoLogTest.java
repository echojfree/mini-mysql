package com.minidb.log;

import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Undo Log 的单元测试
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class UndoLogTest {

    private UndoLogManager undoLogManager;

    @BeforeEach
    void setUp() {
        undoLogManager = new UndoLogManager();
    }

    /**
     * 测试记录 INSERT 操作的 Undo Log
     */
    @Test
    @Order(1)
    void testLogInsert() {
        long undoLogId = undoLogManager.logInsert(1L, "test_table", "row1");

        assertTrue(undoLogId > 0);
        assertEquals(1, undoLogManager.getUndoLogCount());
        assertEquals(1, undoLogManager.getTransactionUndoLogCount(1L));
    }

    /**
     * 测试记录 DELETE 操作的 Undo Log
     */
    @Test
    @Order(2)
    void testLogDelete() {
        byte[] oldData = "old_data".getBytes();
        long undoLogId = undoLogManager.logDelete(1L, "test_table", "row1", oldData);

        assertTrue(undoLogId > 0);
        assertEquals(1, undoLogManager.getUndoLogCount());

        UndoLogRecord record = undoLogManager.getUndoLogs().get(undoLogId);
        assertNotNull(record);
        assertEquals(UndoLogRecord.Type.DELETE, record.getType());
        assertArrayEquals(oldData, record.getOldData());
    }

    /**
     * 测试记录 UPDATE 操作的 Undo Log
     */
    @Test
    @Order(3)
    void testLogUpdate() {
        byte[] oldData = "old_data".getBytes();
        byte[] newData = "new_data".getBytes();
        long undoLogId = undoLogManager.logUpdate(1L, "test_table", "row1", oldData, newData);

        assertTrue(undoLogId > 0);
        assertEquals(1, undoLogManager.getUndoLogCount());

        UndoLogRecord record = undoLogManager.getUndoLogs().get(undoLogId);
        assertNotNull(record);
        assertEquals(UndoLogRecord.Type.UPDATE, record.getType());
        assertArrayEquals(oldData, record.getOldData());
        assertArrayEquals(newData, record.getNewData());
    }

    /**
     * 测试版本链的构建
     */
    @Test
    @Order(4)
    void testVersionChain() {
        // 同一行的多次修改
        undoLogManager.logInsert(1L, "test_table", "row1");
        undoLogManager.logUpdate(2L, "test_table", "row1", "v1".getBytes(), "v2".getBytes());
        undoLogManager.logUpdate(3L, "test_table", "row1", "v2".getBytes(), "v3".getBytes());

        // 获取版本链
        List<UndoLogRecord> versionChain = undoLogManager.getVersionChain("test_table", "row1");

        assertEquals(3, versionChain.size());
        // 版本链应该从新到旧排列
        assertEquals(3L, versionChain.get(0).getTxnId());
        assertEquals(2L, versionChain.get(1).getTxnId());
        assertEquals(1L, versionChain.get(2).getTxnId());
    }

    /**
     * 测试多个事务的 Undo Log
     */
    @Test
    @Order(5)
    void testMultipleTransactions() {
        // 事务 1
        undoLogManager.logInsert(1L, "table1", "row1");
        undoLogManager.logUpdate(1L, "table1", "row1", "v1".getBytes(), "v2".getBytes());

        // 事务 2
        undoLogManager.logInsert(2L, "table2", "row2");

        // 事务 3
        undoLogManager.logDelete(3L, "table3", "row3", "data".getBytes());

        assertEquals(4, undoLogManager.getUndoLogCount());
        assertEquals(2, undoLogManager.getTransactionUndoLogCount(1L));
        assertEquals(1, undoLogManager.getTransactionUndoLogCount(2L));
        assertEquals(1, undoLogManager.getTransactionUndoLogCount(3L));
    }

    /**
     * 测试事务回滚
     */
    @Test
    @Order(6)
    void testRollbackTransaction() {
        // 事务执行多个操作
        undoLogManager.logInsert(1L, "test_table", "row1");
        undoLogManager.logUpdate(1L, "test_table", "row1", "v1".getBytes(), "v2".getBytes());
        undoLogManager.logDelete(1L, "test_table", "row2", "data".getBytes());

        assertEquals(3, undoLogManager.getTransactionUndoLogCount(1L));

        // 回滚事务
        undoLogManager.rollbackTransaction(1L);

        // Undo Log 应该仍然存在（用于 MVCC）
        assertEquals(3, undoLogManager.getUndoLogCount());
    }

    /**
     * 测试 Purge（清理已提交事务的 Undo Log）
     */
    @Test
    @Order(7)
    void testPurgeTransaction() {
        // 事务 1 执行操作
        undoLogManager.logInsert(1L, "test_table", "row1");
        undoLogManager.logUpdate(1L, "test_table", "row1", "v1".getBytes(), "v2".getBytes());

        assertEquals(2, undoLogManager.getUndoLogCount());
        assertEquals(2, undoLogManager.getTransactionUndoLogCount(1L));

        // Purge 事务 1
        undoLogManager.purgeTransaction(1L);

        // Undo Log 应该被清理
        assertEquals(0, undoLogManager.getUndoLogCount());
        assertEquals(0, undoLogManager.getTransactionUndoLogCount(1L));
        assertEquals(2, undoLogManager.getPurgedCount());
    }

    /**
     * 测试版本链的正确性
     */
    @Test
    @Order(8)
    void testVersionChainCorrectness() {
        // 模拟一行数据的多次修改
        long undo1 = undoLogManager.logInsert(1L, "users", "user1");
        long undo2 = undoLogManager.logUpdate(2L, "users", "user1", "name1".getBytes(), "name2".getBytes());
        long undo3 = undoLogManager.logUpdate(3L, "users", "user1", "name2".getBytes(), "name3".getBytes());

        // 获取版本链
        List<UndoLogRecord> chain = undoLogManager.getVersionChain("users", "user1");

        assertEquals(3, chain.size());

        // 验证版本链的指针关系
        UndoLogRecord newest = chain.get(0);
        UndoLogRecord middle = chain.get(1);
        UndoLogRecord oldest = chain.get(2);

        assertEquals(undo3, newest.getUndoLogId());
        assertEquals(undo2, newest.getPreviousUndoLogId());

        assertEquals(undo2, middle.getUndoLogId());
        assertEquals(undo1, middle.getPreviousUndoLogId());

        assertEquals(undo1, oldest.getUndoLogId());
        assertEquals(0, oldest.getPreviousUndoLogId());
    }

    /**
     * 测试空版本链
     */
    @Test
    @Order(9)
    void testEmptyVersionChain() {
        List<UndoLogRecord> chain = undoLogManager.getVersionChain("non_existent_table", "row1");
        assertTrue(chain.isEmpty());
    }

    /**
     * 测试统计信息
     */
    @Test
    @Order(10)
    void testStatistics() {
        // 创建多个事务的 Undo Log
        undoLogManager.logInsert(1L, "table1", "row1");
        undoLogManager.logUpdate(1L, "table1", "row1", "v1".getBytes(), "v2".getBytes());
        undoLogManager.logInsert(2L, "table2", "row2");

        // Purge 事务 1
        undoLogManager.purgeTransaction(1L);

        assertEquals(1, undoLogManager.getUndoLogCount());
        assertEquals(2, undoLogManager.getPurgedCount());

        // 打印统计信息
        undoLogManager.printStats();
    }

    /**
     * 测试并发事务的 Undo Log
     */
    @Test
    @Order(11)
    void testConcurrentTransactions() throws InterruptedException {
        final int txnCount = 10;
        Thread[] threads = new Thread[txnCount];

        for (int i = 0; i < txnCount; i++) {
            final long txnId = i + 1;
            threads[i] = new Thread(() -> {
                undoLogManager.logInsert(txnId, "table", "row_" + txnId);
                undoLogManager.logUpdate(txnId, "table", "row_" + txnId, "old".getBytes(), "new".getBytes());
            });
            threads[i].start();
        }

        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }

        // 每个事务应该有 2 条 Undo Log
        assertEquals(txnCount * 2, undoLogManager.getUndoLogCount());
    }
}
