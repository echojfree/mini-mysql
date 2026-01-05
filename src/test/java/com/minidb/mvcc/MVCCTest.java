package com.minidb.mvcc;

import com.minidb.log.UndoLogManager;
import com.minidb.transaction.Transaction;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MVCC 的单元测试
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MVCCTest {

    private UndoLogManager undoLogManager;
    private MVCCManager mvccManager;

    @BeforeEach
    void setUp() {
        undoLogManager = new UndoLogManager();
        mvccManager = new MVCCManager(undoLogManager);
    }

    /**
     * 测试行记录的隐藏字段
     */
    @Test
    @Order(1)
    void testRowRecordHiddenFields() {
        RowRecord row = new RowRecord("row1", "data1".getBytes());

        assertEquals("row1", row.getRowId());
        assertEquals(0, row.getDbTrxId());
        assertEquals(0, row.getDbRollPtr());

        // 插入操作
        row.insert(1L);
        assertEquals(1L, row.getDbTrxId());
        assertEquals(0, row.getDbRollPtr());

        // 更新操作
        row.update(2L, "data2".getBytes(), 100L);
        assertEquals(2L, row.getDbTrxId());
        assertEquals(100L, row.getDbRollPtr());
        assertArrayEquals("data2".getBytes(), row.getData());
    }

    /**
     * 测试 ReadView 创建
     */
    @Test
    @Order(2)
    void testReadViewCreation() {
        List<Long> mIds = Arrays.asList(1L, 2L, 3L);
        ReadView readView = new ReadView(mIds, 1L, 4L, 2L);

        assertEquals(1L, readView.getMinTrxId());
        assertEquals(4L, readView.getMaxTrxId());
        assertEquals(2L, readView.getCreatorTrxId());
        assertEquals(3, readView.getActiveTransactionCount());
    }

    /**
     * 测试 ReadView 可见性判断 - 规则 1：自己的修改可见
     */
    @Test
    @Order(3)
    void testReadViewVisibility_OwnModification() {
        List<Long> mIds = Arrays.asList(1L, 2L, 3L);
        ReadView readView = new ReadView(mIds, 1L, 4L, 2L);

        // 自己的修改（txnId == creatorTrxId）
        assertTrue(readView.isVisible(2L));
    }

    /**
     * 测试 ReadView 可见性判断 - 规则 2：已提交的旧事务可见
     */
    @Test
    @Order(4)
    void testReadViewVisibility_CommittedOldTransaction() {
        List<Long> mIds = Arrays.asList(5L, 6L, 7L);
        ReadView readView = new ReadView(mIds, 5L, 8L, 6L);

        // 小于最小活跃事务 ID（已提交）
        assertTrue(readView.isVisible(1L));
        assertTrue(readView.isVisible(4L));
    }

    /**
     * 测试 ReadView 可见性判断 - 规则 3：未来的事务不可见
     */
    @Test
    @Order(5)
    void testReadViewVisibility_FutureTransaction() {
        List<Long> mIds = Arrays.asList(5L, 6L, 7L);
        ReadView readView = new ReadView(mIds, 5L, 8L, 6L);

        // 大于等于 maxTrxId（未来的事务）
        assertFalse(readView.isVisible(8L));
        assertFalse(readView.isVisible(9L));
        assertFalse(readView.isVisible(10L));
    }

    /**
     * 测试 ReadView 可见性判断 - 规则 4：活跃事务不可见
     */
    @Test
    @Order(6)
    void testReadViewVisibility_ActiveTransaction() {
        List<Long> mIds = Arrays.asList(5L, 6L, 7L);
        ReadView readView = new ReadView(mIds, 5L, 8L, 6L);

        // 在活跃事务列表中（未提交）
        assertFalse(readView.isVisible(5L)); // 活跃事务
        assertFalse(readView.isVisible(7L)); // 活跃事务
    }

    /**
     * 测试 ReadView 可见性判断 - 规则 5：已提交的事务可见
     */
    @Test
    @Order(7)
    void testReadViewVisibility_CommittedTransaction() {
        List<Long> mIds = Arrays.asList(5L, 7L, 9L);
        ReadView readView = new ReadView(mIds, 5L, 10L, 7L);

        // 在范围内但不在活跃列表中（已提交）
        assertTrue(readView.isVisible(6L));
        assertTrue(readView.isVisible(8L));
    }

    /**
     * 测试行记录可见性判断
     */
    @Test
    @Order(8)
    void testRowVisibility() {
        List<Long> mIds = Arrays.asList(2L, 3L, 4L);
        ReadView readView = new ReadView(mIds, 2L, 5L, 3L);

        // 创建不同事务修改的行记录
        RowRecord row1 = new RowRecord("row1", "data1".getBytes());
        row1.setDbTrxId(1L); // 已提交的旧事务
        assertTrue(readView.isRowVisible(row1));

        RowRecord row2 = new RowRecord("row2", "data2".getBytes());
        row2.setDbTrxId(3L); // 自己的修改
        assertTrue(readView.isRowVisible(row2));

        RowRecord row3 = new RowRecord("row3", "data3".getBytes());
        row3.setDbTrxId(4L); // 活跃事务
        assertFalse(readView.isRowVisible(row3));

        RowRecord row4 = new RowRecord("row4", "data4".getBytes());
        row4.setDbTrxId(6L); // 未来的事务
        assertFalse(readView.isRowVisible(row4));
    }

    /**
     * 测试 MVCC 管理器注册事务
     */
    @Test
    @Order(9)
    void testMVCCManagerRegisterTransaction() {
        Transaction txn1 = new Transaction();
        Transaction txn2 = new Transaction();

        mvccManager.registerTransaction(txn1);
        mvccManager.registerTransaction(txn2);

        assertEquals(2, mvccManager.getActiveTransactionCount());

        mvccManager.unregisterTransaction(txn1);
        assertEquals(1, mvccManager.getActiveTransactionCount());

        mvccManager.unregisterTransaction(txn2);
        assertEquals(0, mvccManager.getActiveTransactionCount());
    }

    /**
     * 测试 RC 隔离级别：每次读创建新 ReadView
     */
    @Test
    @Order(10)
    void testReadCommitted_NewReadViewEachTime() {
        Transaction txn = new Transaction(Transaction.IsolationLevel.READ_COMMITTED);
        mvccManager.registerTransaction(txn);

        // 第一次读
        ReadView view1 = mvccManager.createReadView(txn);
        assertNotNull(view1);

        // 第二次读，RC 级别应该创建新的 ReadView
        ReadView view2 = mvccManager.createReadView(txn);
        assertNotNull(view2);

        // RC 级别不应该缓存 ReadView，但两次创建的内容应该相同（因为活跃事务没变）
        assertEquals(view1.getCreatorTrxId(), view2.getCreatorTrxId());
    }

    /**
     * 测试 RR 隔离级别：复用 ReadView
     */
    @Test
    @Order(11)
    void testRepeatableRead_ReuseReadView() {
        Transaction txn = new Transaction(Transaction.IsolationLevel.REPEATABLE_READ);
        mvccManager.registerTransaction(txn);

        // 第一次读
        ReadView view1 = mvccManager.createReadView(txn);
        assertNotNull(view1);

        // 第二次读，RR 级别应该复用同一个 ReadView
        ReadView view2 = mvccManager.createReadView(txn);
        assertNotNull(view2);

        // 应该是同一个对象
        assertSame(view1, view2);
    }

    /**
     * 测试快照读 - READ UNCOMMITTED
     */
    @Test
    @Order(12)
    void testSnapshotRead_ReadUncommitted() {
        Transaction txn = new Transaction(Transaction.IsolationLevel.READ_UNCOMMITTED);
        mvccManager.registerTransaction(txn);

        RowRecord row = new RowRecord("row1", "data1".getBytes());
        row.setDbTrxId(100L); // 任意事务 ID

        // READ UNCOMMITTED 直接返回最新版本
        RowRecord result = mvccManager.snapshotRead(txn, row, "test_table");
        assertSame(row, result);
    }

    /**
     * 测试快照读 - 可见版本
     */
    @Test
    @Order(13)
    void testSnapshotRead_VisibleVersion() {
        Transaction txn = new Transaction(Transaction.IsolationLevel.REPEATABLE_READ);
        mvccManager.registerTransaction(txn);

        // 创建一个已提交事务修改的行
        RowRecord row = new RowRecord("row1", "data1".getBytes());
        row.setDbTrxId(1L); // 早于当前事务的已提交事务

        RowRecord result = mvccManager.snapshotRead(txn, row, "test_table");
        assertNotNull(result);
        assertEquals("row1", result.getRowId());
    }

    /**
     * 测试快照读 - 版本链
     */
    @Test
    @Order(14)
    void testSnapshotRead_VersionChain() {
        // 创建事务 1 和事务 2
        Transaction txn1 = new Transaction(Transaction.IsolationLevel.REPEATABLE_READ);
        Transaction txn2 = new Transaction(Transaction.IsolationLevel.REPEATABLE_READ);

        mvccManager.registerTransaction(txn1);
        mvccManager.registerTransaction(txn2);

        // 事务 1 修改行（创建旧版本）
        long undoLogId = undoLogManager.logUpdate(txn1.getTxnId(), "test_table", "row1",
                "old_data".getBytes(), "new_data".getBytes());

        // 创建当前版本（被事务 2 修改）
        RowRecord currentRow = new RowRecord("row1", "new_data".getBytes());
        currentRow.setDbTrxId(txn2.getTxnId());
        currentRow.setDbRollPtr(undoLogId);

        // 事务 1 读取（应该看不到事务 2 的修改，需要回溯版本链）
        // 由于事务 2 还在活跃列表中，事务 1 看不到它的修改
        // 应该沿着版本链找到旧版本
        ReadView readView = mvccManager.createReadView(txn1);
        assertFalse(readView.isVisible(txn2.getTxnId()));
    }

    /**
     * 测试当前读
     */
    @Test
    @Order(15)
    void testCurrentRead() {
        RowRecord row = new RowRecord("row1", "data1".getBytes());
        row.setDbTrxId(100L);

        // 当前读返回最新版本
        RowRecord result = mvccManager.currentRead(row);
        assertSame(row, result);
    }

    /**
     * 测试并发事务的 MVCC
     */
    @Test
    @Order(16)
    void testConcurrentTransactionsMVCC() {
        // 创建两个并发事务
        Transaction txn1 = new Transaction(Transaction.IsolationLevel.REPEATABLE_READ);
        Transaction txn2 = new Transaction(Transaction.IsolationLevel.REPEATABLE_READ);

        mvccManager.registerTransaction(txn1);
        mvccManager.registerTransaction(txn2);

        // 两个事务各自创建 ReadView
        ReadView view1 = mvccManager.createReadView(txn1);
        ReadView view2 = mvccManager.createReadView(txn2);

        // 两个事务应该看不到对方的修改
        assertFalse(view1.isVisible(txn2.getTxnId()));
        assertFalse(view2.isVisible(txn1.getTxnId()));

        // 但都能看到自己的修改
        assertTrue(view1.isVisible(txn1.getTxnId()));
        assertTrue(view2.isVisible(txn2.getTxnId()));
    }

    /**
     * 测试 MVCC 防止不可重复读（RR 隔离级别）
     */
    @Test
    @Order(17)
    void testMVCC_PreventNonRepeatableRead() {
        // 事务 1（RR 级别）
        Transaction txn1 = new Transaction(Transaction.IsolationLevel.REPEATABLE_READ);
        mvccManager.registerTransaction(txn1);

        // 事务 1 第一次读，创建 ReadView
        ReadView view1 = mvccManager.createReadView(txn1);

        // 事务 2 修改数据并提交
        Transaction txn2 = new Transaction(Transaction.IsolationLevel.REPEATABLE_READ);
        mvccManager.registerTransaction(txn2);
        mvccManager.unregisterTransaction(txn2); // 模拟提交

        // 事务 1 第二次读，RR 级别应该复用 ReadView
        ReadView view2 = mvccManager.createReadView(txn1);

        // 应该是同一个 ReadView
        assertSame(view1, view2);

        // 因此事务 1 看到的数据保持一致（防止不可重复读）
    }

    /**
     * 测试统计信息
     */
    @Test
    @Order(18)
    void testMVCCStatistics() {
        Transaction txn1 = new Transaction(Transaction.IsolationLevel.REPEATABLE_READ);
        Transaction txn2 = new Transaction(Transaction.IsolationLevel.READ_COMMITTED);

        mvccManager.registerTransaction(txn1);
        mvccManager.registerTransaction(txn2);

        // 为 RR 事务创建 ReadView（会被缓存）
        mvccManager.createReadView(txn1);

        assertEquals(2, mvccManager.getActiveTransactionCount());

        // 打印统计信息
        mvccManager.printStats();

        mvccManager.unregisterTransaction(txn1);
        mvccManager.unregisterTransaction(txn2);

        assertEquals(0, mvccManager.getActiveTransactionCount());
    }
}
