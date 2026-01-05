package com.minidb.transaction;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 事务管理器（TransactionManager）
 *
 * 事务管理器负责管理所有事务的生命周期
 *
 * 核心功能：
 * 1. 开启事务：beginTransaction()
 * 2. 提交事务：commit()
 * 3. 回滚事务：abort()
 * 4. 管理活跃事务列表
 * 5. 生成 Read View（用于 MVCC）
 *
 * 事务管理的关键问题：
 * 1. 如何分配事务 ID？
 *    - 使用全局递增的 ID
 *    - 保证唯一性和顺序性
 *
 * 2. 如何管理活跃事务？
 *    - 维护活跃事务列表
 *    - 用于 MVCC 的可见性判断
 *
 * 3. 如何处理事务提交？
 *    - 释放锁
 *    - 刷盘（持久化）
 *    - 更新事务状态
 *
 * 4. 如何处理事务回滚？
 *    - 撤销修改（Undo）
 *    - 释放锁
 *    - 更新事务状态
 *
 * 对应八股文知识点：
 * ✅ 事务管理器的作用
 * ✅ 事务 ID 的分配策略
 * ✅ 活跃事务列表的作用
 * ✅ 事务提交和回滚的流程
 *
 * @author Mini-MySQL
 */
@Slf4j
@Getter
public class TransactionManager {

    /**
     * 活跃事务表
     * Key: 事务 ID
     * Value: 事务对象
     */
    private final ConcurrentHashMap<Long, Transaction> activeTransactions;

    /**
     * 锁管理器
     */
    private final LockManager lockManager;

    /**
     * 统计信息：已提交事务数
     */
    private long committedCount;

    /**
     * 统计信息：已回滚事务数
     */
    private long abortedCount;

    /**
     * 构造函数
     */
    public TransactionManager() {
        this.activeTransactions = new ConcurrentHashMap<>();
        this.lockManager = new LockManager();
        this.committedCount = 0;
        this.abortedCount = 0;

        log.info("TransactionManager initialized");
    }

    /**
     * 开启新事务（使用默认隔离级别）
     *
     * @return 新事务
     */
    public Transaction beginTransaction() {
        return beginTransaction(Transaction.IsolationLevel.REPEATABLE_READ);
    }

    /**
     * 开启新事务（指定隔离级别）
     *
     * @param isolationLevel 隔离级别
     * @return 新事务
     */
    public Transaction beginTransaction(Transaction.IsolationLevel isolationLevel) {
        Transaction txn = new Transaction(isolationLevel);

        // 生成 Read View（用于 MVCC）
        long minActiveTxnId = getMinActiveTxnId();
        long maxActiveTxnId = txn.getTxnId();
        txn.setReadView(minActiveTxnId, maxActiveTxnId);

        // 加入活跃事务列表
        activeTransactions.put(txn.getTxnId(), txn);

        log.info("Transaction started: {}", txn);

        return txn;
    }

    /**
     * 提交事务
     *
     * @param txn 事务
     */
    public void commit(Transaction txn) {
        if (txn == null) {
            throw new IllegalArgumentException("Transaction cannot be null");
        }

        if (!txn.isActive()) {
            throw new IllegalStateException("Transaction is not active: " + txn);
        }

        try {
            // 1. 提交事务（设置状态为 COMMITTED）
            txn.commit();

            // 2. 释放所有锁
            lockManager.releaseAllLocks(txn);

            // 3. 从活跃事务列表中移除
            activeTransactions.remove(txn.getTxnId());

            // 4. 更新统计信息
            committedCount++;

            log.info("Transaction committed successfully: txnId={}, committedCount={}",
                    txn.getTxnId(), committedCount);

        } catch (Exception e) {
            log.error("Failed to commit transaction: {}", txn, e);
            throw new RuntimeException("Commit failed", e);
        }
    }

    /**
     * 回滚事务
     *
     * @param txn 事务
     */
    public void abort(Transaction txn) {
        if (txn == null) {
            throw new IllegalArgumentException("Transaction cannot be null");
        }

        if (!txn.isActive()) {
            throw new IllegalStateException("Transaction is not active: " + txn);
        }

        try {
            // 1. 回滚事务（设置状态为 ABORTED）
            txn.abort();

            // 2. 撤销修改（简化版：暂不实现）
            // 完整版需要使用 Undo Log 撤销所有修改

            // 3. 释放所有锁
            lockManager.releaseAllLocks(txn);

            // 4. 从活跃事务列表中移除
            activeTransactions.remove(txn.getTxnId());

            // 5. 更新统计信息
            abortedCount++;

            log.info("Transaction aborted successfully: txnId={}, abortedCount={}",
                    txn.getTxnId(), abortedCount);

        } catch (Exception e) {
            log.error("Failed to abort transaction: {}", txn, e);
            throw new RuntimeException("Abort failed", e);
        }
    }

    /**
     * 获取活跃事务数量
     *
     * @return 活跃事务数量
     */
    public int getActiveTransactionCount() {
        return activeTransactions.size();
    }

    /**
     * 获取最小活跃事务 ID
     * 用于生成 Read View
     *
     * @return 最小活跃事务 ID
     */
    private long getMinActiveTxnId() {
        if (activeTransactions.isEmpty()) {
            return Long.MAX_VALUE;
        }

        long minId = Long.MAX_VALUE;
        for (Long txnId : activeTransactions.keySet()) {
            if (txnId < minId) {
                minId = txnId;
            }
        }
        return minId;
    }

    /**
     * 获取事务
     *
     * @param txnId 事务 ID
     * @return 事务对象，如果不存在返回 null
     */
    public Transaction getTransaction(long txnId) {
        return activeTransactions.get(txnId);
    }

    /**
     * 判断事务是否活跃
     *
     * @param txnId 事务 ID
     * @return true 表示活跃
     */
    public boolean isTransactionActive(long txnId) {
        return activeTransactions.containsKey(txnId);
    }

    /**
     * 打印统计信息
     */
    public void printStats() {
        log.info("=== Transaction Manager Statistics ===");
        log.info("Active Transactions: {}", activeTransactions.size());
        log.info("Committed Transactions: {}", committedCount);
        log.info("Aborted Transactions: {}", abortedCount);
        log.info("Total Transactions: {}", committedCount + abortedCount + activeTransactions.size());
        log.info("Lock Count: {}", lockManager.getLockCount());
        log.info("====================================");
    }
}
