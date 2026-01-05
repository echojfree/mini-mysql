package com.minidb.lock;

import com.minidb.transaction.Transaction;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 高级锁管理器（AdvancedLockManager）
 *
 * 实现 InnoDB 的完整锁机制，包括意向锁、间隙锁、Next-Key Lock
 *
 * 锁的类型：
 * 1. 表级锁：
 *    - IS（意向共享锁）：事务打算在表中的某些行上加共享锁
 *    - IX（意向排他锁）：事务打算在表中的某些行上加排他锁
 *    - S（共享锁）：读锁，阻止其他事务修改
 *    - X（排他锁）：写锁，阻止其他事务读写
 *
 * 2. 行级锁：
 *    - Record Lock（记录锁）：锁定单条记录
 *    - Gap Lock（间隙锁）：锁定记录之间的间隙
 *    - Next-Key Lock（临键锁）：Record Lock + Gap Lock
 *    - Insert Intention Lock（插入意向锁）：插入前的间隙锁
 *
 * 意向锁的作用：
 * - 快速判断表级锁是否可以获取
 * - 避免遍历所有行锁
 * - 例如：如果表上有 IX 锁，说明有行被加了 X 锁，此时不能加表级 S 锁
 *
 * 锁兼容矩阵：
 *           IS    IX    S     X
 *    IS     ✓     ✓     ✓     ✗
 *    IX     ✓     ✓     ✗     ✗
 *    S      ✓     ✗     ✓     ✗
 *    X      ✗     ✗     ✗     ✗
 *
 * 间隙锁（Gap Lock）：
 * - 锁定两条记录之间的间隙
 * - 防止幻读（Phantom Read）
 * - 只在 RR 和 SERIALIZABLE 隔离级别生效
 * - 间隙锁之间不冲突（多个事务可以同时持有同一个间隙的间隙锁）
 *
 * Next-Key Lock：
 * - Record Lock + Gap Lock
 * - 锁定记录本身 + 记录前面的间隙
 * - InnoDB 默认的行锁算法
 * - 例如：索引值 10, 20, 30，锁定 20 时实际锁定 (10, 20]
 *
 * 对应八股文知识点：
 * ✅ 意向锁是什么？有什么作用？
 * ✅ 间隙锁如何解决幻读？
 * ✅ Next-Key Lock 的锁定范围
 * ✅ 为什么间隙锁之间不冲突？
 *
 * @author Mini-MySQL
 */
@Slf4j
public class AdvancedLockManager {

    /**
     * 锁类型枚举
     */
    public enum LockType {
        // 表级锁
        IS,         // 意向共享锁
        IX,         // 意向排他锁
        S,          // 共享锁
        X,          // 排他锁

        // 行级锁
        RECORD_S,   // 记录共享锁
        RECORD_X,   // 记录排他锁
        GAP,        // 间隙锁
        NEXT_KEY,   // Next-Key Lock
        INSERT_INTENTION  // 插入意向锁
    }

    /**
     * 锁对象
     */
    @Data
    public static class Lock {
        private long txnId;              // 持有锁的事务 ID
        private LockType lockType;       // 锁类型
        private String resource;         // 资源标识（表名或行ID）
        private Long gapStart;           // 间隙锁的起始值（可选）
        private Long gapEnd;             // 间隙锁的结束值（可选）
        private long timestamp;          // 加锁时间

        public Lock(long txnId, LockType lockType, String resource) {
            this.txnId = txnId;
            this.lockType = lockType;
            this.resource = resource;
            this.timestamp = System.currentTimeMillis();
        }

        public Lock(long txnId, LockType lockType, String resource, Long gapStart, Long gapEnd) {
            this(txnId, lockType, resource);
            this.gapStart = gapStart;
            this.gapEnd = gapEnd;
        }

        @Override
        public String toString() {
            if (gapStart != null && gapEnd != null) {
                return String.format("Lock{txn=%d, type=%s, resource='%s', gap=(%d,%d)}",
                        txnId, lockType, resource, gapStart, gapEnd);
            }
            return String.format("Lock{txn=%d, type=%s, resource='%s'}",
                    txnId, lockType, resource);
        }
    }

    /**
     * 锁表
     * Key: 资源标识（表名或 表名:行ID）
     * Value: 该资源上的所有锁列表
     */
    private final Map<String, List<Lock>> lockTable;

    /**
     * 事务持有的锁
     * Key: 事务 ID
     * Value: 该事务持有的所有锁
     */
    private final Map<Long, List<Lock>> transactionLocks;

    /**
     * 锁兼容矩阵
     * 行：已持有的锁
     * 列：请求的锁
     */
    private final boolean[][] compatibilityMatrix;

    /**
     * 构造函数
     */
    public AdvancedLockManager() {
        this.lockTable = new ConcurrentHashMap<>();
        this.transactionLocks = new ConcurrentHashMap<>();
        this.compatibilityMatrix = buildCompatibilityMatrix();

        log.info("AdvancedLockManager initialized");
    }

    /**
     * 构建锁兼容矩阵
     *
     * @return 兼容矩阵
     */
    private boolean[][] buildCompatibilityMatrix() {
        // 矩阵顺序：IS, IX, S, X
        boolean[][] matrix = new boolean[4][4];

        // IS 与所有锁兼容，除了 X
        matrix[0][0] = true;  // IS + IS
        matrix[0][1] = true;  // IS + IX
        matrix[0][2] = true;  // IS + S
        matrix[0][3] = false; // IS + X

        // IX 与 IS, IX 兼容
        matrix[1][0] = true;  // IX + IS
        matrix[1][1] = true;  // IX + IX
        matrix[1][2] = false; // IX + S
        matrix[1][3] = false; // IX + X

        // S 与 IS, S 兼容
        matrix[2][0] = true;  // S + IS
        matrix[2][1] = false; // S + IX
        matrix[2][2] = true;  // S + S
        matrix[2][3] = false; // S + X

        // X 与所有锁不兼容
        matrix[3][0] = false; // X + IS
        matrix[3][1] = false; // X + IX
        matrix[3][2] = false; // X + S
        matrix[3][3] = false; // X + X

        return matrix;
    }

    /**
     * 判断两个锁是否兼容
     *
     * @param held      已持有的锁
     * @param requested 请求的锁
     * @return true 表示兼容
     */
    private boolean isCompatible(LockType held, LockType requested) {
        // 表级锁兼容性
        if (isTableLock(held) && isTableLock(requested)) {
            int heldIndex = getTableLockIndex(held);
            int requestedIndex = getTableLockIndex(requested);
            return compatibilityMatrix[heldIndex][requestedIndex];
        }

        // 间隙锁之间总是兼容的（关键特性！）
        if (held == LockType.GAP && requested == LockType.GAP) {
            return true;
        }

        // 记录锁和间隙锁的兼容性
        // Record S + Record S: 兼容
        if (held == LockType.RECORD_S && requested == LockType.RECORD_S) {
            return true;
        }

        // 其他情况：Record X 与任何锁不兼容
        return false;
    }

    /**
     * 判断是否为表级锁
     */
    private boolean isTableLock(LockType lockType) {
        return lockType == LockType.IS || lockType == LockType.IX ||
               lockType == LockType.S || lockType == LockType.X;
    }

    /**
     * 获取表级锁在兼容矩阵中的索引
     */
    private int getTableLockIndex(LockType lockType) {
        switch (lockType) {
            case IS: return 0;
            case IX: return 1;
            case S:  return 2;
            case X:  return 3;
            default: throw new IllegalArgumentException("Not a table lock: " + lockType);
        }
    }

    /**
     * 获取表级意向锁
     *
     * @param txn       事务
     * @param tableName 表名
     * @param lockType  锁类型（IS 或 IX）
     * @return 是否成功
     */
    public synchronized boolean acquireTableIntentionLock(Transaction txn, String tableName, LockType lockType) {
        if (lockType != LockType.IS && lockType != LockType.IX) {
            throw new IllegalArgumentException("Invalid intention lock type: " + lockType);
        }

        String resource = "table:" + tableName;

        // 检查是否已持有该锁（避免重复添加）
        if (holdsLock(txn.getTxnId(), resource, lockType)) {
            return true; // 已持有该锁，直接返回成功
        }

        // 检查兼容性
        if (!checkCompatibility(txn.getTxnId(), resource, lockType)) {
            log.debug("Cannot acquire {} lock on table '{}' for txn {}: incompatible locks exist",
                    lockType, tableName, txn.getTxnId());
            return false;
        }

        // 获取锁
        Lock lock = new Lock(txn.getTxnId(), lockType, resource);
        addLock(resource, lock, txn.getTxnId());

        log.debug("Acquired {} lock on table '{}' for txn {}", lockType, tableName, txn.getTxnId());
        return true;
    }

    /**
     * 获取表级锁
     *
     * @param txn       事务
     * @param tableName 表名
     * @param lockType  锁类型（S 或 X）
     * @return 是否成功
     */
    public synchronized boolean acquireTableLock(Transaction txn, String tableName, LockType lockType) {
        if (lockType != LockType.S && lockType != LockType.X) {
            throw new IllegalArgumentException("Invalid table lock type: " + lockType);
        }

        String resource = "table:" + tableName;

        // 检查兼容性
        if (!checkCompatibility(txn.getTxnId(), resource, lockType)) {
            log.debug("Cannot acquire {} lock on table '{}' for txn {}: incompatible locks exist",
                    lockType, tableName, txn.getTxnId());
            return false;
        }

        // 获取锁
        Lock lock = new Lock(txn.getTxnId(), lockType, resource);
        addLock(resource, lock, txn.getTxnId());

        log.debug("Acquired {} lock on table '{}' for txn {}", lockType, tableName, txn.getTxnId());
        return true;
    }

    /**
     * 获取记录锁
     *
     * @param txn       事务
     * @param tableName 表名
     * @param rowId     行 ID
     * @param lockType  锁类型（RECORD_S 或 RECORD_X）
     * @return 是否成功
     */
    public synchronized boolean acquireRecordLock(Transaction txn, String tableName, String rowId, LockType lockType) {
        if (lockType != LockType.RECORD_S && lockType != LockType.RECORD_X) {
            throw new IllegalArgumentException("Invalid record lock type: " + lockType);
        }

        // 先获取表级意向锁
        LockType intentionLock = (lockType == LockType.RECORD_S) ? LockType.IS : LockType.IX;
        if (!acquireTableIntentionLock(txn, tableName, intentionLock)) {
            return false;
        }

        String resource = "row:" + tableName + ":" + rowId;

        // 检查兼容性
        if (!checkCompatibility(txn.getTxnId(), resource, lockType)) {
            log.debug("Cannot acquire {} lock on row '{}:{}' for txn {}: incompatible locks exist",
                    lockType, tableName, rowId, txn.getTxnId());
            return false;
        }

        // 获取锁
        Lock lock = new Lock(txn.getTxnId(), lockType, resource);
        addLock(resource, lock, txn.getTxnId());

        log.debug("Acquired {} lock on row '{}:{}' for txn {}", lockType, tableName, rowId, txn.getTxnId());
        return true;
    }

    /**
     * 获取间隙锁
     *
     * @param txn       事务
     * @param tableName 表名
     * @param gapStart  间隙起始值
     * @param gapEnd    间隙结束值
     * @return 是否成功
     */
    public synchronized boolean acquireGapLock(Transaction txn, String tableName, long gapStart, long gapEnd) {
        // 先获取表级 IX 锁
        if (!acquireTableIntentionLock(txn, tableName, LockType.IX)) {
            return false;
        }

        String resource = "gap:" + tableName + ":" + gapStart + "-" + gapEnd;

        // 间隙锁之间总是兼容的，所以不需要检查兼容性
        Lock lock = new Lock(txn.getTxnId(), LockType.GAP, resource, gapStart, gapEnd);
        addLock(resource, lock, txn.getTxnId());

        log.debug("Acquired GAP lock on table '{}' gap ({},{}) for txn {}",
                tableName, gapStart, gapEnd, txn.getTxnId());
        return true;
    }

    /**
     * 获取 Next-Key Lock
     *
     * @param txn       事务
     * @param tableName 表名
     * @param rowId     行 ID
     * @param gapStart  间隙起始值
     * @param gapEnd    间隙结束值（行记录的值）
     * @return 是否成功
     */
    public synchronized boolean acquireNextKeyLock(Transaction txn, String tableName, String rowId,
                                                    long gapStart, long gapEnd) {
        // Next-Key Lock = Record Lock + Gap Lock
        // 先获取记录锁
        if (!acquireRecordLock(txn, tableName, rowId, LockType.RECORD_X)) {
            return false;
        }

        // 再获取间隙锁
        if (!acquireGapLock(txn, tableName, gapStart, gapEnd)) {
            return false;
        }

        log.debug("Acquired NEXT-KEY lock on row '{}:{}' gap ({},{}) for txn {}",
                tableName, rowId, gapStart, gapEnd, txn.getTxnId());
        return true;
    }

    /**
     * 检查事务是否已持有指定资源的指定类型锁
     *
     * @param txnId    事务 ID
     * @param resource 资源标识
     * @param lockType 锁类型
     * @return true 表示已持有
     */
    private boolean holdsLock(long txnId, String resource, LockType lockType) {
        List<Lock> locks = lockTable.get(resource);
        if (locks == null) {
            return false;
        }

        for (Lock lock : locks) {
            if (lock.getTxnId() == txnId && lock.getLockType() == lockType) {
                return true;
            }
        }
        return false;
    }

    /**
     * 检查锁兼容性
     *
     * @param txnId     请求锁的事务 ID
     * @param resource  资源标识
     * @param requested 请求的锁类型
     * @return true 表示兼容
     */
    private boolean checkCompatibility(long txnId, String resource, LockType requested) {
        List<Lock> locks = lockTable.get(resource);
        if (locks == null || locks.isEmpty()) {
            return true; // 没有其他锁，兼容
        }

        for (Lock lock : locks) {
            // 跳过自己持有的锁
            if (lock.getTxnId() == txnId) {
                continue;
            }

            // 检查兼容性
            if (!isCompatible(lock.getLockType(), requested)) {
                return false;
            }
        }

        return true;
    }

    /**
     * 添加锁
     */
    private void addLock(String resource, Lock lock, long txnId) {
        // 添加到锁表
        lockTable.computeIfAbsent(resource, k -> new ArrayList<>()).add(lock);

        // 添加到事务锁列表
        transactionLocks.computeIfAbsent(txnId, k -> new ArrayList<>()).add(lock);
    }

    /**
     * 释放事务持有的所有锁
     *
     * @param txn 事务
     */
    public synchronized void releaseAllLocks(Transaction txn) {
        List<Lock> locks = transactionLocks.remove(txn.getTxnId());
        if (locks == null || locks.isEmpty()) {
            return;
        }

        for (Lock lock : locks) {
            List<Lock> resourceLocks = lockTable.get(lock.getResource());
            if (resourceLocks != null) {
                resourceLocks.remove(lock);
                if (resourceLocks.isEmpty()) {
                    lockTable.remove(lock.getResource());
                }
            }
        }

        log.debug("Released {} locks for txn {}", locks.size(), txn.getTxnId());
    }

    /**
     * 获取事务持有的锁数量
     *
     * @param txnId 事务 ID
     * @return 锁数量
     */
    public int getTransactionLockCount(long txnId) {
        List<Lock> locks = transactionLocks.get(txnId);
        return locks == null ? 0 : locks.size();
    }

    /**
     * 获取总锁数量
     *
     * @return 锁数量
     */
    public int getTotalLockCount() {
        return lockTable.values().stream().mapToInt(List::size).sum();
    }

    /**
     * 打印统计信息
     */
    public void printStats() {
        log.info("=== Advanced Lock Manager Statistics ===");
        log.info("Total Resources Locked: {}", lockTable.size());
        log.info("Total Locks: {}", getTotalLockCount());
        log.info("Active Transactions with Locks: {}", transactionLocks.size());
        log.info("========================================");
    }
}
