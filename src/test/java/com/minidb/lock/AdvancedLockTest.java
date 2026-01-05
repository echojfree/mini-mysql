package com.minidb.lock;

import com.minidb.transaction.Transaction;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 高级锁机制的单元测试
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AdvancedLockTest {

    private AdvancedLockManager lockManager;
    private Transaction txn1;
    private Transaction txn2;
    private Transaction txn3;

    @BeforeEach
    void setUp() {
        lockManager = new AdvancedLockManager();
        txn1 = new Transaction();
        txn2 = new Transaction();
        txn3 = new Transaction();
    }

    /**
     * 测试意向共享锁（IS）
     */
    @Test
    @Order(1)
    void testIntentionSharedLock() {
        boolean acquired = lockManager.acquireTableIntentionLock(txn1, "users", AdvancedLockManager.LockType.IS);
        assertTrue(acquired);
        assertEquals(1, lockManager.getTransactionLockCount(txn1.getTxnId()));
    }

    /**
     * 测试意向排他锁（IX）
     */
    @Test
    @Order(2)
    void testIntentionExclusiveLock() {
        boolean acquired = lockManager.acquireTableIntentionLock(txn1, "users", AdvancedLockManager.LockType.IX);
        assertTrue(acquired);
        assertEquals(1, lockManager.getTransactionLockCount(txn1.getTxnId()));
    }

    /**
     * 测试锁兼容性：IS + IS 兼容
     */
    @Test
    @Order(3)
    void testLockCompatibility_IS_IS() {
        lockManager.acquireTableIntentionLock(txn1, "users", AdvancedLockManager.LockType.IS);
        boolean acquired = lockManager.acquireTableIntentionLock(txn2, "users", AdvancedLockManager.LockType.IS);
        assertTrue(acquired, "IS + IS should be compatible");
    }

    /**
     * 测试锁兼容性：IS + IX 兼容
     */
    @Test
    @Order(4)
    void testLockCompatibility_IS_IX() {
        lockManager.acquireTableIntentionLock(txn1, "users", AdvancedLockManager.LockType.IS);
        boolean acquired = lockManager.acquireTableIntentionLock(txn2, "users", AdvancedLockManager.LockType.IX);
        assertTrue(acquired, "IS + IX should be compatible");
    }

    /**
     * 测试锁兼容性：IS + S 兼容
     */
    @Test
    @Order(5)
    void testLockCompatibility_IS_S() {
        lockManager.acquireTableIntentionLock(txn1, "users", AdvancedLockManager.LockType.IS);
        boolean acquired = lockManager.acquireTableLock(txn2, "users", AdvancedLockManager.LockType.S);
        assertTrue(acquired, "IS + S should be compatible");
    }

    /**
     * 测试锁兼容性：IS + X 不兼容
     */
    @Test
    @Order(6)
    void testLockCompatibility_IS_X() {
        lockManager.acquireTableIntentionLock(txn1, "users", AdvancedLockManager.LockType.IS);
        boolean acquired = lockManager.acquireTableLock(txn2, "users", AdvancedLockManager.LockType.X);
        assertFalse(acquired, "IS + X should not be compatible");
    }

    /**
     * 测试锁兼容性：IX + S 不兼容
     */
    @Test
    @Order(7)
    void testLockCompatibility_IX_S() {
        lockManager.acquireTableIntentionLock(txn1, "users", AdvancedLockManager.LockType.IX);
        boolean acquired = lockManager.acquireTableLock(txn2, "users", AdvancedLockManager.LockType.S);
        assertFalse(acquired, "IX + S should not be compatible");
    }

    /**
     * 测试锁兼容性：S + S 兼容
     */
    @Test
    @Order(8)
    void testLockCompatibility_S_S() {
        lockManager.acquireTableLock(txn1, "users", AdvancedLockManager.LockType.S);
        boolean acquired = lockManager.acquireTableLock(txn2, "users", AdvancedLockManager.LockType.S);
        assertTrue(acquired, "S + S should be compatible");
    }

    /**
     * 测试锁兼容性：X + X 不兼容
     */
    @Test
    @Order(9)
    void testLockCompatibility_X_X() {
        lockManager.acquireTableLock(txn1, "users", AdvancedLockManager.LockType.X);
        boolean acquired = lockManager.acquireTableLock(txn2, "users", AdvancedLockManager.LockType.X);
        assertFalse(acquired, "X + X should not be compatible");
    }

    /**
     * 测试记录锁（Record Lock）
     */
    @Test
    @Order(10)
    void testRecordLock() {
        boolean acquired = lockManager.acquireRecordLock(txn1, "users", "row1",
                AdvancedLockManager.LockType.RECORD_S);
        assertTrue(acquired);

        // 应该自动获取表级 IS 锁 + 行级 RECORD_S 锁
        assertEquals(2, lockManager.getTransactionLockCount(txn1.getTxnId()));
    }

    /**
     * 测试记录锁兼容性：RECORD_S + RECORD_S 兼容
     */
    @Test
    @Order(11)
    void testRecordLockCompatibility_S_S() {
        lockManager.acquireRecordLock(txn1, "users", "row1", AdvancedLockManager.LockType.RECORD_S);
        boolean acquired = lockManager.acquireRecordLock(txn2, "users", "row1",
                AdvancedLockManager.LockType.RECORD_S);
        assertTrue(acquired, "RECORD_S + RECORD_S should be compatible");
    }

    /**
     * 测试记录锁兼容性：RECORD_S + RECORD_X 不兼容
     */
    @Test
    @Order(12)
    void testRecordLockCompatibility_S_X() {
        lockManager.acquireRecordLock(txn1, "users", "row1", AdvancedLockManager.LockType.RECORD_S);
        boolean acquired = lockManager.acquireRecordLock(txn2, "users", "row1",
                AdvancedLockManager.LockType.RECORD_X);
        assertFalse(acquired, "RECORD_S + RECORD_X should not be compatible");
    }

    /**
     * 测试间隙锁（Gap Lock）
     */
    @Test
    @Order(13)
    void testGapLock() {
        boolean acquired = lockManager.acquireGapLock(txn1, "users", 10, 20);
        assertTrue(acquired);

        // 应该自动获取表级 IX 锁 + 间隙锁
        assertEquals(2, lockManager.getTransactionLockCount(txn1.getTxnId()));
    }

    /**
     * 测试间隙锁兼容性：GAP + GAP 兼容（关键特性！）
     */
    @Test
    @Order(14)
    void testGapLockCompatibility() {
        lockManager.acquireGapLock(txn1, "users", 10, 20);
        boolean acquired = lockManager.acquireGapLock(txn2, "users", 10, 20);
        assertTrue(acquired, "Gap locks should be compatible with each other");
    }

    /**
     * 测试 Next-Key Lock
     */
    @Test
    @Order(15)
    void testNextKeyLock() {
        boolean acquired = lockManager.acquireNextKeyLock(txn1, "users", "row20", 10, 20);
        assertTrue(acquired);

        // 应该获取：表级 IX 锁 + 记录锁 + 间隙锁
        assertEquals(3, lockManager.getTransactionLockCount(txn1.getTxnId()));
    }

    /**
     * 测试释放所有锁
     */
    @Test
    @Order(16)
    void testReleaseAllLocks() {
        lockManager.acquireTableLock(txn1, "users", AdvancedLockManager.LockType.S);
        lockManager.acquireRecordLock(txn1, "users", "row1", AdvancedLockManager.LockType.RECORD_S);
        lockManager.acquireGapLock(txn1, "users", 10, 20);

        int lockCount = lockManager.getTransactionLockCount(txn1.getTxnId());
        assertTrue(lockCount > 0);

        lockManager.releaseAllLocks(txn1);
        assertEquals(0, lockManager.getTransactionLockCount(txn1.getTxnId()));
    }

    /**
     * 测试多个事务的锁
     */
    @Test
    @Order(17)
    void testMultipleTransactionLocks() {
        lockManager.acquireTableLock(txn1, "users", AdvancedLockManager.LockType.S);
        lockManager.acquireTableLock(txn2, "users", AdvancedLockManager.LockType.S);
        lockManager.acquireRecordLock(txn3, "products", "row1", AdvancedLockManager.LockType.RECORD_X);

        assertTrue(lockManager.getTransactionLockCount(txn1.getTxnId()) > 0);
        assertTrue(lockManager.getTransactionLockCount(txn2.getTxnId()) > 0);
        assertTrue(lockManager.getTransactionLockCount(txn3.getTxnId()) > 0);
        assertTrue(lockManager.getTotalLockCount() >= 4);
    }

    /**
     * 测试统计信息
     */
    @Test
    @Order(18)
    void testStatistics() {
        lockManager.acquireTableLock(txn1, "users", AdvancedLockManager.LockType.S);
        lockManager.acquireRecordLock(txn2, "users", "row1", AdvancedLockManager.LockType.RECORD_X);

        assertTrue(lockManager.getTotalLockCount() > 0);

        // 打印统计信息
        lockManager.printStats();
    }
}
