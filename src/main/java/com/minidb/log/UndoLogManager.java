package com.minidb.log;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Undo Log 管理器（UndoLogManager）
 *
 * 负责管理事务的 Undo Log，支持事务回滚和 MVCC
 *
 * 核心功能：
 * 1. 记录 Undo Log：为每个数据修改操作生成 Undo 记录
 * 2. 事务回滚：使用 Undo Log 撤销事务的所有修改
 * 3. 版本链管理：维护数据的多版本链表
 * 4. 日志清理：清理已提交事务的 Undo Log（Purge）
 *
 * Undo Log 的存储：
 * 1. 简化版：使用内存 HashMap 存储
 * 2. 完整版：使用 Undo 页存储到磁盘
 *
 * 版本链（Version Chain）：
 * - 每个数据行的多个版本通过 Undo Log 形成链表
 * - 链表按事务 ID 降序排列（最新版本在前）
 * - MVCC 通过版本链找到可见的历史版本
 *
 * 对应八股文知识点：
 * ✅ Undo Log 如何组织和存储？
 * ✅ 如何使用 Undo Log 实现事务回滚？
 * ✅ MVCC 的版本链是如何构建的？
 * ✅ Undo Log 何时可以被清理（Purge）？
 *
 * @author Mini-MySQL
 */
@Slf4j
@Getter
public class UndoLogManager {

    /**
     * Undo Log ID 生成器
     */
    private final AtomicLong undoLogIdGenerator;

    /**
     * Undo Log 存储
     * Key: Undo Log ID
     * Value: Undo Log 记录
     */
    private final ConcurrentHashMap<Long, UndoLogRecord> undoLogs;

    /**
     * 事务的 Undo Log 列表
     * Key: 事务 ID
     * Value: 该事务的所有 Undo Log ID 列表
     */
    private final ConcurrentHashMap<Long, List<Long>> txnUndoLogs;

    /**
     * 版本链
     * Key: 表名:行ID
     * Value: 最新的 Undo Log ID（链表头）
     */
    private final ConcurrentHashMap<String, Long> versionChains;

    /**
     * 统计信息：已清理的 Undo Log 数量
     */
    private long purgedCount;

    /**
     * 构造函数
     */
    public UndoLogManager() {
        this.undoLogIdGenerator = new AtomicLong(1);
        this.undoLogs = new ConcurrentHashMap<>();
        this.txnUndoLogs = new ConcurrentHashMap<>();
        this.versionChains = new ConcurrentHashMap<>();
        this.purgedCount = 0;

        log.info("UndoLogManager initialized");
    }

    /**
     * 记录 INSERT 操作的 Undo Log
     *
     * @param txnId     事务 ID
     * @param tableName 表名
     * @param rowId     行 ID
     * @return Undo Log ID
     */
    public long logInsert(long txnId, String tableName, String rowId) {
        UndoLogRecord record = UndoLogRecord.createInsertUndo(txnId, tableName, rowId);
        return addUndoLog(record);
    }

    /**
     * 记录 DELETE 操作的 Undo Log
     *
     * @param txnId     事务 ID
     * @param tableName 表名
     * @param rowId     行 ID
     * @param oldData   删除的完整行数据
     * @return Undo Log ID
     */
    public long logDelete(long txnId, String tableName, String rowId, byte[] oldData) {
        UndoLogRecord record = UndoLogRecord.createDeleteUndo(txnId, tableName, rowId, oldData);
        return addUndoLog(record);
    }

    /**
     * 记录 UPDATE 操作的 Undo Log
     *
     * @param txnId     事务 ID
     * @param tableName 表名
     * @param rowId     行 ID
     * @param oldData   修改前的数据
     * @param newData   修改后的数据
     * @return Undo Log ID
     */
    public long logUpdate(long txnId, String tableName, String rowId, byte[] oldData, byte[] newData) {
        UndoLogRecord record = UndoLogRecord.createUpdateUndo(txnId, tableName, rowId, oldData, newData);
        return addUndoLog(record);
    }

    /**
     * 添加 Undo Log 记录
     *
     * @param record Undo Log 记录
     * @return Undo Log ID
     */
    private long addUndoLog(UndoLogRecord record) {
        // 生成 Undo Log ID
        long undoLogId = undoLogIdGenerator.getAndIncrement();
        record.setUndoLogId(undoLogId);

        // 构建版本链
        String key = record.getTableName() + ":" + record.getRowId();
        Long previousUndoId = versionChains.get(key);
        if (previousUndoId != null) {
            record.setPreviousUndoLogId(previousUndoId);
        }
        versionChains.put(key, undoLogId);

        // 存储 Undo Log
        undoLogs.put(undoLogId, record);

        // 记录事务的 Undo Log 列表
        txnUndoLogs.computeIfAbsent(record.getTxnId(), k -> new ArrayList<>()).add(undoLogId);

        log.debug("Added Undo Log: {}", record);
        return undoLogId;
    }

    /**
     * 回滚事务
     * 使用该事务的所有 Undo Log 撤销修改
     *
     * @param txnId 事务 ID
     */
    public void rollbackTransaction(long txnId) {
        List<Long> undoLogIds = txnUndoLogs.get(txnId);
        if (undoLogIds == null || undoLogIds.isEmpty()) {
            log.info("No Undo Logs found for transaction: txnId={}", txnId);
            return;
        }

        log.info("Rolling back transaction: txnId={}, undoLogCount={}", txnId, undoLogIds.size());

        // 按逆序回滚（先回滚最后的操作）
        for (int i = undoLogIds.size() - 1; i >= 0; i--) {
            long undoLogId = undoLogIds.get(i);
            UndoLogRecord record = undoLogs.get(undoLogId);

            if (record == null) {
                log.warn("Undo Log not found: undoLogId={}", undoLogId);
                continue;
            }

            // 执行回滚操作
            applyUndo(record);
        }

        log.info("Transaction rolled back successfully: txnId={}", txnId);
    }

    /**
     * 应用单条 Undo Log（执行撤销操作）
     *
     * @param record Undo Log 记录
     */
    private void applyUndo(UndoLogRecord record) {
        switch (record.getType()) {
            case INSERT:
                // INSERT 的撤销：删除该行
                log.debug("Undo INSERT: table={}, rowId={}", record.getTableName(), record.getRowId());
                // 实际应该调用存储引擎删除该行
                break;

            case DELETE:
                // DELETE 的撤销：重新插入该行
                log.debug("Undo DELETE: table={}, rowId={}", record.getTableName(), record.getRowId());
                // 实际应该调用存储引擎插入 oldData
                break;

            case UPDATE:
                // UPDATE 的撤销：恢复旧值
                log.debug("Undo UPDATE: table={}, rowId={}", record.getTableName(), record.getRowId());
                // 实际应该调用存储引擎更新为 oldData
                break;
        }
    }

    /**
     * 清理已提交事务的 Undo Log（Purge）
     * 当事务提交后，且没有其他事务需要该版本时，可以清理
     *
     * @param txnId 事务 ID
     */
    public void purgeTransaction(long txnId) {
        List<Long> undoLogIds = txnUndoLogs.remove(txnId);
        if (undoLogIds == null || undoLogIds.isEmpty()) {
            return;
        }

        for (Long undoLogId : undoLogIds) {
            UndoLogRecord record = undoLogs.remove(undoLogId);
            if (record != null) {
                purgedCount++;
                log.debug("Purged Undo Log: {}", record);
            }
        }

        log.info("Purged Undo Logs for transaction: txnId={}, count={}", txnId, undoLogIds.size());
    }

    /**
     * 获取指定行的版本链
     *
     * @param tableName 表名
     * @param rowId     行 ID
     * @return Undo Log 版本链（从新到旧）
     */
    public List<UndoLogRecord> getVersionChain(String tableName, String rowId) {
        List<UndoLogRecord> chain = new ArrayList<>();
        String key = tableName + ":" + rowId;
        Long currentUndoId = versionChains.get(key);

        while (currentUndoId != null) {
            UndoLogRecord record = undoLogs.get(currentUndoId);
            if (record == null) {
                break;
            }
            chain.add(record);
            currentUndoId = record.getPreviousUndoLogId() > 0 ? record.getPreviousUndoLogId() : null;
        }

        return chain;
    }

    /**
     * 获取 Undo Log 数量
     *
     * @return Undo Log 数量
     */
    public int getUndoLogCount() {
        return undoLogs.size();
    }

    /**
     * 获取事务的 Undo Log 数量
     *
     * @param txnId 事务 ID
     * @return 该事务的 Undo Log 数量
     */
    public int getTransactionUndoLogCount(long txnId) {
        List<Long> undoLogIds = txnUndoLogs.get(txnId);
        return undoLogIds == null ? 0 : undoLogIds.size();
    }

    /**
     * 打印统计信息
     */
    public void printStats() {
        log.info("=== Undo Log Manager Statistics ===");
        log.info("Total Undo Logs: {}", undoLogs.size());
        log.info("Active Transactions: {}", txnUndoLogs.size());
        log.info("Version Chains: {}", versionChains.size());
        log.info("Purged Count: {}", purgedCount);
        log.info("===================================");
    }
}
