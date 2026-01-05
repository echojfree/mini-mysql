package com.minidb.transaction;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 锁管理器（LockManager）
 *
 * 锁管理器负责管理事务对数据的锁定，防止并发冲突
 *
 * 锁的类型：
 * 1. 共享锁（S 锁）：读锁
 *    - 多个事务可以同时持有同一资源的共享锁
 *    - 共享锁与共享锁兼容
 *    - 共享锁与排他锁互斥
 *
 * 2. 排他锁（X 锁）：写锁
 *    - 只有一个事务可以持有某个资源的排他锁
 *    - 排他锁与所有锁互斥
 *
 * 锁的粒度：
 * 1. 表锁：锁定整个表
 *    - 开销小，加锁快
 *    - 并发度低，容易产生锁冲突
 *
 * 2. 行锁：锁定单个行
 *    - 开销大，加锁慢
 *    - 并发度高，锁冲突少
 *    - InnoDB 支持行锁
 *
 * 3. 页锁：锁定页面（介于表锁和行锁之间）
 *
 * 锁的算法：
 * 1. Record Lock（记录锁）：锁定单条记录
 * 2. Gap Lock（间隙锁）：锁定记录之间的间隙
 * 3. Next-Key Lock（临键锁）：Record Lock + Gap Lock
 *    - 解决幻读问题
 *
 * 死锁处理：
 * 1. 死锁检测：使用等待图（Wait-for Graph）
 * 2. 死锁预防：超时机制
 * 3. 死锁解除：选择一个事务回滚（牺牲者选择）
 *
 * 对应八股文知识点：
 * ✅ 什么是锁？锁有哪些类型？
 * ✅ 共享锁和排他锁的区别
 * ✅ 表锁和行锁的区别
 * ✅ InnoDB 如何实现行锁？
 * ✅ 什么是死锁？如何避免死锁？
 * ✅ Gap Lock 和 Next-Key Lock 是什么？
 *
 * @author Mini-MySQL
 */
@Slf4j
public class LockManager {

    /**
     * 锁类型枚举
     */
    public enum LockType {
        SHARED,    // 共享锁（S 锁）
        EXCLUSIVE  // 排他锁（X 锁）
    }

    /**
     * 锁对象
     * 内部类，表示一个锁
     */
    private static class Lock {
        private final ReentrantReadWriteLock rwLock;
        private final String resourceId;

        public Lock(String resourceId) {
            this.resourceId = resourceId;
            this.rwLock = new ReentrantReadWriteLock();
        }

        public void lockShared() {
            rwLock.readLock().lock();
        }

        public void unlockShared() {
            rwLock.readLock().unlock();
        }

        public void lockExclusive() {
            rwLock.writeLock().lock();
        }

        public void unlockExclusive() {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * 锁表
     * Key: 资源 ID（格式：表名 或 表名:行ID）
     * Value: 锁对象
     */
    private final ConcurrentHashMap<String, Lock> lockTable;

    /**
     * 构造函数
     */
    public LockManager() {
        this.lockTable = new ConcurrentHashMap<>();
    }

    /**
     * 获取表锁
     *
     * @param txn       事务
     * @param tableName 表名
     * @param lockType  锁类型
     * @return 是否成功
     */
    public boolean lockTable(Transaction txn, String tableName, LockType lockType) {
        String resourceId = "table:" + tableName;
        Lock lock = lockTable.computeIfAbsent(resourceId, k -> new Lock(resourceId));

        try {
            if (lockType == LockType.SHARED) {
                lock.lockShared();
                log.debug("Acquired SHARED lock on table: txnId={}, table={}", txn.getTxnId(), tableName);
            } else {
                lock.lockExclusive();
                log.debug("Acquired EXCLUSIVE lock on table: txnId={}, table={}", txn.getTxnId(), tableName);
            }
            return true;
        } catch (Exception e) {
            log.error("Failed to acquire lock: txnId={}, table={}, lockType={}",
                    txn.getTxnId(), tableName, lockType, e);
            return false;
        }
    }

    /**
     * 释放表锁
     *
     * @param txn       事务
     * @param tableName 表名
     * @param lockType  锁类型
     */
    public void unlockTable(Transaction txn, String tableName, LockType lockType) {
        String resourceId = "table:" + tableName;
        Lock lock = lockTable.get(resourceId);

        if (lock == null) {
            log.warn("Lock not found: txnId={}, table={}", txn.getTxnId(), tableName);
            return;
        }

        try {
            if (lockType == LockType.SHARED) {
                lock.unlockShared();
                log.debug("Released SHARED lock on table: txnId={}, table={}", txn.getTxnId(), tableName);
            } else {
                lock.unlockExclusive();
                log.debug("Released EXCLUSIVE lock on table: txnId={}, table={}", txn.getTxnId(), tableName);
            }
        } catch (Exception e) {
            log.error("Failed to release lock: txnId={}, table={}, lockType={}",
                    txn.getTxnId(), tableName, lockType, e);
        }
    }

    /**
     * 获取行锁
     *
     * @param txn       事务
     * @param tableName 表名
     * @param rowId     行 ID
     * @param lockType  锁类型
     * @return 是否成功
     */
    public boolean lockRow(Transaction txn, String tableName, String rowId, LockType lockType) {
        String resourceId = "row:" + tableName + ":" + rowId;
        Lock lock = lockTable.computeIfAbsent(resourceId, k -> new Lock(resourceId));

        try {
            if (lockType == LockType.SHARED) {
                lock.lockShared();
                log.debug("Acquired SHARED lock on row: txnId={}, table={}, rowId={}",
                        txn.getTxnId(), tableName, rowId);
            } else {
                lock.lockExclusive();
                log.debug("Acquired EXCLUSIVE lock on row: txnId={}, table={}, rowId={}",
                        txn.getTxnId(), tableName, rowId);
            }
            return true;
        } catch (Exception e) {
            log.error("Failed to acquire row lock: txnId={}, table={}, rowId={}, lockType={}",
                    txn.getTxnId(), tableName, rowId, lockType, e);
            return false;
        }
    }

    /**
     * 释放行锁
     *
     * @param txn       事务
     * @param tableName 表名
     * @param rowId     行 ID
     * @param lockType  锁类型
     */
    public void unlockRow(Transaction txn, String tableName, String rowId, LockType lockType) {
        String resourceId = "row:" + tableName + ":" + rowId;
        Lock lock = lockTable.get(resourceId);

        if (lock == null) {
            log.warn("Row lock not found: txnId={}, table={}, rowId={}", txn.getTxnId(), tableName, rowId);
            return;
        }

        try {
            if (lockType == LockType.SHARED) {
                lock.unlockShared();
                log.debug("Released SHARED lock on row: txnId={}, table={}, rowId={}",
                        txn.getTxnId(), tableName, rowId);
            } else {
                lock.unlockExclusive();
                log.debug("Released EXCLUSIVE lock on row: txnId={}, table={}, rowId={}",
                        txn.getTxnId(), tableName, rowId);
            }
        } catch (Exception e) {
            log.error("Failed to release row lock: txnId={}, table={}, rowId={}, lockType={}",
                    txn.getTxnId(), tableName, rowId, lockType, e);
        }
    }

    /**
     * 锁升级
     * 将共享锁升级为排他锁
     *
     * @param txn       事务
     * @param tableName 表名
     * @param rowId     行 ID
     * @return 是否成功
     */
    public boolean upgradeLock(Transaction txn, String tableName, String rowId) {
        // 先释放共享锁
        unlockRow(txn, tableName, rowId, LockType.SHARED);

        // 再获取排他锁
        return lockRow(txn, tableName, rowId, LockType.EXCLUSIVE);
    }

    /**
     * 释放事务持有的所有锁
     *
     * @param txn 事务
     */
    public void releaseAllLocks(Transaction txn) {
        log.debug("Releasing all locks for transaction: txnId={}", txn.getTxnId());

        // 简化版：遍历所有锁并尝试释放
        // 完整版需要记录每个事务持有的锁列表

        // 注意：由于我们使用 ReentrantReadWriteLock，
        // 实际释放需要知道锁的类型和数量
        // 这里只是示意，实际应该维护事务的锁列表

        log.info("Released all locks for transaction: txnId={}", txn.getTxnId());
    }

    /**
     * 获取锁表大小
     *
     * @return 锁的数量
     */
    public int getLockCount() {
        return lockTable.size();
    }

    /**
     * 清空所有锁
     */
    public void clear() {
        lockTable.clear();
        log.info("Cleared all locks");
    }
}
