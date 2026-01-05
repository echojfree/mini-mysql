package com.minidb.transaction;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 事务（Transaction）
 *
 * 事务是数据库操作的基本单元，满足 ACID 特性
 *
 * ACID 特性：
 * 1. Atomicity（原子性）：事务中的所有操作要么全部完成，要么全部不完成
 * 2. Consistency（一致性）：事务必须使数据库从一个一致性状态变换到另一个一致性状态
 * 3. Isolation（隔离性）：多个事务并发执行时，一个事务的执行不应影响其他事务
 * 4. Durability（持久性）：事务一旦提交，对数据库的改变就是永久性的
 *
 * 事务状态：
 * - ACTIVE：活跃状态，事务正在执行
 * - COMMITTED：已提交，事务执行成功
 * - ABORTED：已回滚，事务执行失败
 *
 * 事务隔离级别（从低到高）：
 * 1. READ UNCOMMITTED（读未提交）
 *    - 最低隔离级别，允许读取未提交的数据
 *    - 问题：脏读、不可重复读、幻读
 *
 * 2. READ COMMITTED（读已提交）
 *    - 只能读取已提交的数据
 *    - 问题：不可重复读、幻读
 *    - MySQL 默认级别（Oracle、PostgreSQL）
 *
 * 3. REPEATABLE READ（可重复读）
 *    - 同一事务中多次读取同一数据结果相同
 *    - 问题：幻读
 *    - MySQL InnoDB 默认级别
 *
 * 4. SERIALIZABLE（串行化）
 *    - 最高隔离级别，事务串行执行
 *    - 问题：性能最差
 *
 * 并发问题：
 * 1. 脏读（Dirty Read）：读取到其他事务未提交的数据
 * 2. 不可重复读（Non-Repeatable Read）：同一事务中多次读取同一数据，结果不同
 * 3. 幻读（Phantom Read）：同一事务中多次查询，结果集的数量不同
 *
 * 对应八股文知识点：
 * ✅ 什么是事务？ACID 特性是什么？
 * ✅ 事务的隔离级别有哪些？
 * ✅ 脏读、不可重复读、幻读是什么？
 * ✅ MySQL 的默认隔离级别是什么？
 * ✅ 为什么 InnoDB 选择 REPEATABLE READ？
 *
 * @author Mini-MySQL
 */
@Slf4j
@Data
public class Transaction {

    /**
     * 事务状态枚举
     */
    public enum State {
        ACTIVE,      // 活跃
        COMMITTED,   // 已提交
        ABORTED      // 已回滚
    }

    /**
     * 事务隔离级别枚举
     */
    public enum IsolationLevel {
        READ_UNCOMMITTED(0),  // 读未提交
        READ_COMMITTED(1),    // 读已提交
        REPEATABLE_READ(2),   // 可重复读
        SERIALIZABLE(3);      // 串行化

        private final int level;

        IsolationLevel(int level) {
            this.level = level;
        }

        public int getLevel() {
            return level;
        }
    }

    /**
     * 全局事务 ID 生成器
     * 使用原子长整型保证线程安全
     */
    private static final AtomicLong txnIdGenerator = new AtomicLong(1);

    /**
     * 事务 ID
     * 全局唯一，递增分配
     */
    private final long txnId;

    /**
     * 事务状态
     */
    private State state;

    /**
     * 事务隔离级别
     */
    private IsolationLevel isolationLevel;

    /**
     * 事务开始时间
     */
    private final long startTime;

    /**
     * 事务结束时间
     */
    private long endTime;

    /**
     * Read View
     * 用于 MVCC，记录事务开始时的活跃事务列表
     * 简化版：只记录最小和最大事务 ID
     */
    private long minActiveTxnId;  // 最小活跃事务 ID
    private long maxActiveTxnId;  // 最大活跃事务 ID

    /**
     * 构造函数（使用默认隔离级别）
     */
    public Transaction() {
        this(IsolationLevel.REPEATABLE_READ);
    }

    /**
     * 构造函数（指定隔离级别）
     *
     * @param isolationLevel 隔离级别
     */
    public Transaction(IsolationLevel isolationLevel) {
        this.txnId = txnIdGenerator.getAndIncrement();
        this.state = State.ACTIVE;
        this.isolationLevel = isolationLevel;
        this.startTime = System.currentTimeMillis();
        this.endTime = 0;
        this.minActiveTxnId = txnId;
        this.maxActiveTxnId = txnId;

        log.debug("Transaction started: txnId={}, isolationLevel={}", txnId, isolationLevel);
    }

    /**
     * 提交事务
     */
    public void commit() {
        if (state != State.ACTIVE) {
            throw new IllegalStateException("Transaction is not active: txnId=" + txnId + ", state=" + state);
        }

        this.state = State.COMMITTED;
        this.endTime = System.currentTimeMillis();

        long duration = endTime - startTime;
        log.info("Transaction committed: txnId={}, duration={}ms", txnId, duration);
    }

    /**
     * 回滚事务
     */
    public void abort() {
        if (state != State.ACTIVE) {
            throw new IllegalStateException("Transaction is not active: txnId=" + txnId + ", state=" + state);
        }

        this.state = State.ABORTED;
        this.endTime = System.currentTimeMillis();

        long duration = endTime - startTime;
        log.info("Transaction aborted: txnId={}, duration={}ms", txnId, duration);
    }

    /**
     * 判断事务是否活跃
     *
     * @return true 表示活跃
     */
    public boolean isActive() {
        return state == State.ACTIVE;
    }

    /**
     * 判断事务是否已提交
     *
     * @return true 表示已提交
     */
    public boolean isCommitted() {
        return state == State.COMMITTED;
    }

    /**
     * 判断事务是否已回滚
     *
     * @return true 表示已回滚
     */
    public boolean isAborted() {
        return state == State.ABORTED;
    }

    /**
     * 设置 Read View
     * 用于 MVCC 可见性判断
     *
     * @param minActiveTxnId 最小活跃事务 ID
     * @param maxActiveTxnId 最大活跃事务 ID
     */
    public void setReadView(long minActiveTxnId, long maxActiveTxnId) {
        this.minActiveTxnId = minActiveTxnId;
        this.maxActiveTxnId = maxActiveTxnId;
        log.debug("Set read view: txnId={}, min={}, max={}", txnId, minActiveTxnId, maxActiveTxnId);
    }

    /**
     * 判断某个版本是否对当前事务可见（MVCC）
     *
     * 可见性规则：
     * 1. 如果版本的 txnId 等于当前事务 ID，可见（自己创建的）
     * 2. 如果版本的 txnId < minActiveTxnId，可见（已提交的旧事务）
     * 3. 如果版本的 txnId > maxActiveTxnId，不可见（未来的事务）
     * 4. 如果版本的 txnId 在 [minActiveTxnId, maxActiveTxnId] 之间，
     *    需要检查该事务是否已提交（简化版：不可见）
     *
     * @param versionTxnId 版本的事务 ID
     * @return true 表示可见
     */
    public boolean isVisible(long versionTxnId) {
        // 自己创建的版本，可见
        if (versionTxnId == this.txnId) {
            return true;
        }

        // 版本创建于所有活跃事务之前，可见
        if (versionTxnId < this.minActiveTxnId) {
            return true;
        }

        // 版本创建于所有活跃事务之后，不可见
        if (versionTxnId > this.maxActiveTxnId) {
            return false;
        }

        // 版本在活跃事务范围内，简化版：不可见
        // 完整版需要检查该事务是否已提交
        return false;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "txnId=" + txnId +
                ", state=" + state +
                ", isolationLevel=" + isolationLevel +
                ", duration=" + (endTime > 0 ? (endTime - startTime) + "ms" : "active") +
                '}';
    }
}
