package com.minidb.mvcc;

import com.minidb.log.UndoLogManager;
import com.minidb.log.UndoLogRecord;
import com.minidb.transaction.Transaction;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MVCC 管理器（MVCCManager）
 *
 * 负责管理 ReadView 的创建和快照读的实现
 *
 * 核心功能：
 * 1. 创建 ReadView：根据隔离级别决定创建时机
 *    - RC（读已提交）：每次读取时创建新 ReadView
 *    - RR（可重复读）：事务开始时创建一次，之后复用
 *
 * 2. 快照读（Snapshot Read）：
 *    - 使用 ReadView 判断版本可见性
 *    - 沿着版本链找到第一个可见版本
 *    - 不加锁，高并发
 *
 * 3. 当前读（Current Read）：
 *    - 读取最新版本
 *    - 需要加锁（S 锁或 X 锁）
 *    - 例如：SELECT ... FOR UPDATE
 *
 * 隔离级别实现：
 * - READ UNCOMMITTED：不使用 MVCC，读最新版本（脏读）
 * - READ COMMITTED：每次读创建 ReadView
 * - REPEATABLE READ：事务开始时创建 ReadView
 * - SERIALIZABLE：使用锁机制
 *
 * 对应八股文知识点：
 * ✅ 快照读和当前读的区别
 * ✅ RC 和 RR 的 ReadView 创建时机
 * ✅ 如何通过版本链实现 MVCC
 * ✅ 为什么 RR 能防止不可重复读
 *
 * @author Mini-MySQL
 */
@Slf4j
@Getter
public class MVCCManager {

    /**
     * 活跃事务表
     * Key: 事务 ID
     * Value: 事务对象
     */
    private final Map<Long, Transaction> activeTransactions;

    /**
     * 事务的 ReadView 缓存
     * Key: 事务 ID
     * Value: ReadView
     * 用于 RR 隔离级别复用 ReadView
     */
    private final Map<Long, ReadView> readViewCache;

    /**
     * Undo Log 管理器（用于读取历史版本）
     */
    private final UndoLogManager undoLogManager;

    /**
     * 下一个事务 ID
     */
    private long nextTrxId;

    /**
     * 构造函数
     *
     * @param undoLogManager Undo Log 管理器
     */
    public MVCCManager(UndoLogManager undoLogManager) {
        this.activeTransactions = new ConcurrentHashMap<>();
        this.readViewCache = new ConcurrentHashMap<>();
        this.undoLogManager = undoLogManager;
        this.nextTrxId = 1;

        log.info("MVCCManager initialized");
    }

    /**
     * 注册活跃事务
     *
     * @param txn 事务
     */
    public void registerTransaction(Transaction txn) {
        activeTransactions.put(txn.getTxnId(), txn);
        nextTrxId = Math.max(nextTrxId, txn.getTxnId() + 1);
        log.debug("Registered active transaction: txnId={}", txn.getTxnId());
    }

    /**
     * 注销事务（提交或回滚后）
     *
     * @param txn 事务
     */
    public void unregisterTransaction(Transaction txn) {
        activeTransactions.remove(txn.getTxnId());
        readViewCache.remove(txn.getTxnId());
        log.debug("Unregistered transaction: txnId={}", txn.getTxnId());
    }

    /**
     * 创建 ReadView
     *
     * 根据隔离级别决定创建策略：
     * - RC：每次读取时创建新 ReadView
     * - RR：事务开始时创建一次，之后复用
     *
     * @param txn 事务
     * @return ReadView
     */
    public ReadView createReadView(Transaction txn) {
        Transaction.IsolationLevel level = txn.getIsolationLevel();

        // RR 隔离级别：复用已创建的 ReadView
        if (level == Transaction.IsolationLevel.REPEATABLE_READ) {
            ReadView cachedView = readViewCache.get(txn.getTxnId());
            if (cachedView != null) {
                log.debug("Reusing cached ReadView for RR: txnId={}", txn.getTxnId());
                return cachedView;
            }
        }

        // 创建新的 ReadView
        ReadView readView = buildReadView(txn.getTxnId());

        // RR 隔离级别：缓存 ReadView
        if (level == Transaction.IsolationLevel.REPEATABLE_READ) {
            readViewCache.put(txn.getTxnId(), readView);
            log.debug("Cached ReadView for RR: txnId={}", txn.getTxnId());
        }

        return readView;
    }

    /**
     * 构建 ReadView
     *
     * @param creatorTxnId 创建者事务 ID
     * @return ReadView
     */
    private ReadView buildReadView(long creatorTxnId) {
        // 获取所有活跃事务 ID
        List<Long> mIds = new ArrayList<>(activeTransactions.keySet());

        // 计算最小活跃事务 ID
        long minTrxId = mIds.isEmpty() ? nextTrxId : mIds.stream().min(Long::compare).orElse(nextTrxId);

        // 下一个事务 ID
        long maxTrxId = nextTrxId;

        return new ReadView(mIds, minTrxId, maxTrxId, creatorTxnId);
    }

    /**
     * 快照读（Snapshot Read）
     *
     * 根据 ReadView 沿着版本链查找第一个可见版本
     *
     * @param txn   事务
     * @param row   当前行记录
     * @param table 表名
     * @return 可见的行记录版本，如果没有可见版本返回 null
     */
    public RowRecord snapshotRead(Transaction txn, RowRecord row, String table) {
        // READ UNCOMMITTED：直接返回最新版本（脏读）
        if (txn.getIsolationLevel() == Transaction.IsolationLevel.READ_UNCOMMITTED) {
            log.debug("READ_UNCOMMITTED: returning latest version");
            return row;
        }

        // 创建或获取 ReadView
        ReadView readView = createReadView(txn);

        // 沿着版本链查找可见版本
        return findVisibleVersion(row, table, readView);
    }

    /**
     * 沿着版本链查找第一个可见版本
     *
     * @param currentRow 当前行记录
     * @param table      表名
     * @param readView   ReadView
     * @return 可见的行记录版本，如果没有返回 null
     */
    private RowRecord findVisibleVersion(RowRecord currentRow, String table, ReadView readView) {
        RowRecord row = currentRow;

        // 沿着版本链回溯
        while (row != null) {
            // 判断当前版本是否可见
            if (readView.isVisible(row.getDbTrxId())) {
                log.debug("Found visible version: rowId={}, dbTrxId={}",
                        row.getRowId(), row.getDbTrxId());
                return row;
            }

            // 版本不可见，沿着回滚指针找上一个版本
            if (row.getDbRollPtr() > 0) {
                row = getOldVersion(table, row);
            } else {
                // 没有更旧的版本了
                row = null;
            }
        }

        log.debug("No visible version found for row: {}", currentRow.getRowId());
        return null;
    }

    /**
     * 从 Undo Log 获取旧版本
     *
     * @param table      表名
     * @param currentRow 当前行记录
     * @return 旧版本行记录，如果不存在返回 null
     */
    private RowRecord getOldVersion(String table, RowRecord currentRow) {
        long undoLogId = currentRow.getDbRollPtr();
        if (undoLogId == 0) {
            return null;
        }

        // 从 Undo Log 读取旧版本
        UndoLogRecord undoLog = undoLogManager.getUndoLogs().get(undoLogId);
        if (undoLog == null) {
            log.warn("Undo Log not found: undoLogId={}", undoLogId);
            return null;
        }

        // 根据 Undo Log 重建旧版本
        RowRecord oldRow = new RowRecord(currentRow.getRowId(), undoLog.getOldData());
        oldRow.setDbTrxId(undoLog.getTxnId());
        oldRow.setDbRollPtr(undoLog.getPreviousUndoLogId());

        log.trace("Reconstructed old version from Undo Log: undoLogId={}, dbTrxId={}",
                undoLogId, oldRow.getDbTrxId());

        return oldRow;
    }

    /**
     * 当前读（Current Read）
     *
     * 读取最新版本，需要加锁
     *
     * @param row 行记录
     * @return 最新版本
     */
    public RowRecord currentRead(RowRecord row) {
        log.debug("Current read: rowId={}, latest dbTrxId={}", row.getRowId(), row.getDbTrxId());
        // 当前读直接返回最新版本
        // 实际应该先加锁（S 锁或 X 锁）
        return row;
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
     * 打印统计信息
     */
    public void printStats() {
        log.info("=== MVCC Manager Statistics ===");
        log.info("Active Transactions: {}", activeTransactions.size());
        log.info("Cached ReadViews: {}", readViewCache.size());
        log.info("Next TxnId: {}", nextTrxId);
        log.info("==============================");
    }
}
