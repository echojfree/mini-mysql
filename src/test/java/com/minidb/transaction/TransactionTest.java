package com.minidb.transaction;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 事务管理的单元测试
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TransactionTest {

    private TransactionManager txnManager;

    @BeforeEach
    void setUp() {
        txnManager = new TransactionManager();
    }

    /**
     * 测试创建事务
     */
    @Test
    @Order(1)
    void testBeginTransaction() {
        Transaction txn = txnManager.beginTransaction();

        assertNotNull(txn);
        assertTrue(txn.getTxnId() > 0);
        assertEquals(Transaction.State.ACTIVE, txn.getState());
        assertEquals(Transaction.IsolationLevel.REPEATABLE_READ, txn.getIsolationLevel());
        assertTrue(txn.isActive());
        assertEquals(1, txnManager.getActiveTransactionCount());
    }

    /**
     * 测试创建多个事务
     */
    @Test
    @Order(2)
    void testBeginMultipleTransactions() {
        Transaction txn1 = txnManager.beginTransaction();
        Transaction txn2 = txnManager.beginTransaction();
        Transaction txn3 = txnManager.beginTransaction();

        assertNotEquals(txn1.getTxnId(), txn2.getTxnId());
        assertNotEquals(txn2.getTxnId(), txn3.getTxnId());
        assertTrue(txn1.getTxnId() < txn2.getTxnId());
        assertTrue(txn2.getTxnId() < txn3.getTxnId());
        assertEquals(3, txnManager.getActiveTransactionCount());
    }

    /**
     * 测试提交事务
     */
    @Test
    @Order(3)
    void testCommitTransaction() {
        Transaction txn = txnManager.beginTransaction();
        assertEquals(1, txnManager.getActiveTransactionCount());

        txnManager.commit(txn);

        assertEquals(Transaction.State.COMMITTED, txn.getState());
        assertTrue(txn.isCommitted());
        assertFalse(txn.isActive());
        assertEquals(0, txnManager.getActiveTransactionCount());
        assertEquals(1, txnManager.getCommittedCount());
    }

    /**
     * 测试回滚事务
     */
    @Test
    @Order(4)
    void testAbortTransaction() {
        Transaction txn = txnManager.beginTransaction();
        assertEquals(1, txnManager.getActiveTransactionCount());

        txnManager.abort(txn);

        assertEquals(Transaction.State.ABORTED, txn.getState());
        assertTrue(txn.isAborted());
        assertFalse(txn.isActive());
        assertEquals(0, txnManager.getActiveTransactionCount());
        assertEquals(1, txnManager.getAbortedCount());
    }

    /**
     * 测试事务隔离级别
     */
    @Test
    @Order(5)
    void testIsolationLevels() {
        Transaction txn1 = txnManager.beginTransaction(Transaction.IsolationLevel.READ_UNCOMMITTED);
        Transaction txn2 = txnManager.beginTransaction(Transaction.IsolationLevel.READ_COMMITTED);
        Transaction txn3 = txnManager.beginTransaction(Transaction.IsolationLevel.REPEATABLE_READ);
        Transaction txn4 = txnManager.beginTransaction(Transaction.IsolationLevel.SERIALIZABLE);

        assertEquals(Transaction.IsolationLevel.READ_UNCOMMITTED, txn1.getIsolationLevel());
        assertEquals(Transaction.IsolationLevel.READ_COMMITTED, txn2.getIsolationLevel());
        assertEquals(Transaction.IsolationLevel.REPEATABLE_READ, txn3.getIsolationLevel());
        assertEquals(Transaction.IsolationLevel.SERIALIZABLE, txn4.getIsolationLevel());

        assertEquals(4, txnManager.getActiveTransactionCount());
    }

    /**
     * 测试 MVCC 可见性判断
     */
    @Test
    @Order(6)
    void testMVCCVisibility() {
        Transaction txn1 = txnManager.beginTransaction();
        Transaction txn2 = txnManager.beginTransaction();
        Transaction txn3 = txnManager.beginTransaction();

        long txn1Id = txn1.getTxnId();
        long txn2Id = txn2.getTxnId();
        long txn3Id = txn3.getTxnId();

        // txn2 的可见性判断
        // 自己创建的版本可见
        assertTrue(txn2.isVisible(txn2Id));

        // 比自己早的事务不可见（活跃事务）
        assertFalse(txn2.isVisible(txn1Id));

        // 比自己晚的事务不可见
        assertFalse(txn2.isVisible(txn3Id));

        // 提交 txn1
        txnManager.commit(txn1);

        // 创建新事务 txn4
        Transaction txn4 = txnManager.beginTransaction();

        // txn4 应该可以看到 txn1（已提交且小于 minActiveTxnId）
        assertTrue(txn4.isVisible(txn1Id));

        // txn4 不应该看到 txn2 和 txn3（活跃事务）
        assertFalse(txn4.isVisible(txn2Id));
        assertFalse(txn4.isVisible(txn3Id));
    }

    /**
     * 测试表锁
     */
    @Test
    @Order(7)
    void testTableLock() {
        Transaction txn = txnManager.beginTransaction();
        LockManager lockManager = txnManager.getLockManager();

        // 获取共享锁
        boolean locked = lockManager.lockTable(txn, "test_table", LockManager.LockType.SHARED);
        assertTrue(locked);

        // 释放共享锁
        lockManager.unlockTable(txn, "test_table", LockManager.LockType.SHARED);

        // 获取排他锁
        locked = lockManager.lockTable(txn, "test_table", LockManager.LockType.EXCLUSIVE);
        assertTrue(locked);

        // 释放排他锁
        lockManager.unlockTable(txn, "test_table", LockManager.LockType.EXCLUSIVE);
    }

    /**
     * 测试行锁
     */
    @Test
    @Order(8)
    void testRowLock() {
        Transaction txn = txnManager.beginTransaction();
        LockManager lockManager = txnManager.getLockManager();

        // 获取行的共享锁
        boolean locked = lockManager.lockRow(txn, "test_table", "row1", LockManager.LockType.SHARED);
        assertTrue(locked);

        // 释放行的共享锁
        lockManager.unlockRow(txn, "test_table", "row1", LockManager.LockType.SHARED);

        // 获取行的排他锁
        locked = lockManager.lockRow(txn, "test_table", "row1", LockManager.LockType.EXCLUSIVE);
        assertTrue(locked);

        // 释放行的排他锁
        lockManager.unlockRow(txn, "test_table", "row1", LockManager.LockType.EXCLUSIVE);
    }

    /**
     * 测试锁升级
     */
    @Test
    @Order(9)
    void testLockUpgrade() {
        Transaction txn = txnManager.beginTransaction();
        LockManager lockManager = txnManager.getLockManager();

        // 先获取共享锁
        lockManager.lockRow(txn, "test_table", "row1", LockManager.LockType.SHARED);

        // 升级为排他锁
        boolean upgraded = lockManager.upgradeLock(txn, "test_table", "row1");
        assertTrue(upgraded);

        // 释放排他锁
        lockManager.unlockRow(txn, "test_table", "row1", LockManager.LockType.EXCLUSIVE);
    }

    /**
     * 测试并发事务
     */
    @Test
    @Order(10)
    void testConcurrentTransactions() throws InterruptedException {
        final int txnCount = 10;
        Thread[] threads = new Thread[txnCount];

        for (int i = 0; i < txnCount; i++) {
            threads[i] = new Thread(() -> {
                Transaction txn = txnManager.beginTransaction();
                try {
                    // 模拟事务执行
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                txnManager.commit(txn);
            });
            threads[i].start();
        }

        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }

        // 所有事务应该都已提交
        assertEquals(0, txnManager.getActiveTransactionCount());
        assertEquals(txnCount, txnManager.getCommittedCount());
    }

    /**
     * 测试异常：提交非活跃事务
     */
    @Test
    @Order(11)
    void testCommitInactiveTransaction() {
        Transaction txn = txnManager.beginTransaction();
        txnManager.commit(txn);

        // 再次提交应该抛出异常
        assertThrows(IllegalStateException.class, () -> {
            txnManager.commit(txn);
        });
    }

    /**
     * 测试异常：回滚非活跃事务
     */
    @Test
    @Order(12)
    void testAbortInactiveTransaction() {
        Transaction txn = txnManager.beginTransaction();
        txnManager.abort(txn);

        // 再次回滚应该抛出异常
        assertThrows(IllegalStateException.class, () -> {
            txnManager.abort(txn);
        });
    }

    /**
     * 测试统计信息
     */
    @Test
    @Order(13)
    void testStatistics() {
        // 创建并提交一些事务
        for (int i = 0; i < 5; i++) {
            Transaction txn = txnManager.beginTransaction();
            txnManager.commit(txn);
        }

        // 创建并回滚一些事务
        for (int i = 0; i < 3; i++) {
            Transaction txn = txnManager.beginTransaction();
            txnManager.abort(txn);
        }

        // 创建一些活跃事务
        for (int i = 0; i < 2; i++) {
            txnManager.beginTransaction();
        }

        assertEquals(2, txnManager.getActiveTransactionCount());
        assertEquals(5, txnManager.getCommittedCount());
        assertEquals(3, txnManager.getAbortedCount());

        // 打印统计信息
        txnManager.printStats();
    }

    /**
     * 测试 Read View
     */
    @Test
    @Order(14)
    void testReadView() {
        Transaction txn1 = txnManager.beginTransaction();
        Transaction txn2 = txnManager.beginTransaction();

        // txn2 的 Read View 应该包含 txn1
        long minActiveTxnId = txn2.getMinActiveTxnId();
        long maxActiveTxnId = txn2.getMaxActiveTxnId();

        assertTrue(minActiveTxnId <= txn1.getTxnId());
        assertEquals(txn2.getTxnId(), maxActiveTxnId);
    }
}
