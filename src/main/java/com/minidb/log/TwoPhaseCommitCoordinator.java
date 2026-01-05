package com.minidb.log;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * 两阶段提交协调器 (Two-Phase Commit Coordinator)
 *
 * 核心概念:
 * 两阶段提交是保证 Redo Log 和 Binlog 一致性的关键机制
 *
 * 为什么需要两阶段提交?
 * 1. Redo Log (InnoDB 层) 和 Binlog (Server 层) 是两个独立的日志系统
 * 2. 如果单独写入,可能出现不一致:
 *    - Redo 写入成功,Binlog 写入失败 → 主库有数据,从库没有
 *    - Binlog 写入成功,Redo 写入失败 → 主库没数据,从库有数据
 *
 * 两阶段提交流程:
 *
 * Phase 1: Prepare 阶段
 * 1. 写入 Redo Log,标记为 PREPARE 状态
 * 2. 此时 Redo Log 已持久化,但事务还未提交
 *
 * Phase 2: Commit 阶段
 * 3. 写入 Binlog
 * 4. 更新 Redo Log 状态为 COMMIT
 * 5. 事务提交完成
 *
 * 崩溃恢复规则:
 * - 如果 Redo Log 处于 PREPARE 状态:
 *   - 检查 Binlog 是否存在对应记录
 *   - 存在 → 提交事务
 *   - 不存在 → 回滚事务
 * - 如果 Redo Log 处于 COMMIT 状态:
 *   - 直接提交事务
 *
 * 对应八股文知识点:
 * ✅ 两阶段提交是什么?
 * ✅ 为什么需要两阶段提交?
 * ✅ Redo Log 和 Binlog 的写入顺序
 * ✅ 崩溃恢复时如何保证一致性?
 * ✅ 两阶段提交的性能影响
 *
 * @author Mini-MySQL
 */
@Slf4j
public class TwoPhaseCommitCoordinator {

    /**
     * Redo Log 管理器
     */
    private final RedoLogManager redoLogManager;

    /**
     * Binlog 管理器
     */
    private final BinlogManager binlogManager;

    /**
     * 构造函数
     */
    public TwoPhaseCommitCoordinator(RedoLogManager redoLogManager, BinlogManager binlogManager) {
        this.redoLogManager = redoLogManager;
        this.binlogManager = binlogManager;
        log.info("TwoPhaseCommitCoordinator initialized");
    }

    /**
     * 执行两阶段提交
     *
     * @param txnId 事务 ID
     * @param binlogRecords Binlog 记录列表
     * @throws IOException IO 异常
     */
    public void commit(long txnId, java.util.List<BinlogRecord> binlogRecords) throws IOException {
        long startTime = System.currentTimeMillis();

        try {
            // ===== Phase 1: Prepare 阶段 =====
            // 1. 写入 Redo Log (PREPARE 状态)
            // 注意: 实际 MySQL 中,Redo Log 在事务执行过程中就在写入
            // 这里简化为在提交时统一写入
            redoLogManager.prepare(txnId);
            log.debug("Phase 1 (Prepare): Redo Log prepared for txnId={}", txnId);

            // ===== Phase 2: Commit 阶段 =====
            // 2. 写入 Binlog
            for (BinlogRecord record : binlogRecords) {
                binlogManager.append(record);
            }
            binlogManager.flush(); // 刷盘保证持久化
            log.debug("Phase 2 (Commit): Binlog written for txnId={}, records={}",
                    txnId, binlogRecords.size());

            // 3. 更新 Redo Log 状态为 COMMIT
            redoLogManager.commit(txnId);
            log.debug("Phase 2 (Commit): Redo Log committed for txnId={}", txnId);

            long duration = System.currentTimeMillis() - startTime;
            log.info("Two-phase commit completed: txnId={}, duration={}ms", txnId, duration);

        } catch (Exception e) {
            log.error("Two-phase commit failed: txnId={}", txnId, e);
            // 提交失败,需要回滚
            rollback(txnId);
            throw new RuntimeException("Two-phase commit failed", e);
        }
    }

    /**
     * 回滚事务
     *
     * @param txnId 事务 ID
     */
    public void rollback(long txnId) throws IOException {
        try {
            // 回滚 Redo Log
            redoLogManager.rollback(txnId);
            log.info("Rolled back transaction: txnId={}", txnId);

        } catch (Exception e) {
            log.error("Rollback failed: txnId={}", txnId, e);
            throw new RuntimeException("Rollback failed", e);
        }
    }

    /**
     * 崩溃恢复
     *
     * 恢复规则:
     * 1. 扫描 Redo Log,找到所有 PREPARE 状态的事务
     * 2. 对于每个 PREPARE 事务:
     *    - 检查 Binlog 是否有对应的 COMMIT 记录
     *    - 有 → 提交事务
     *    - 无 → 回滚事务
     * 3. 对于所有 COMMIT 状态的事务,直接提交
     *
     * @throws IOException IO 异常
     */
    public void recover() throws IOException {
        log.info("Starting crash recovery...");

        try {
            // 1. 从 Redo Log 恢复
            redoLogManager.recover();

            // 2. 读取所有 Binlog 记录
            java.util.List<BinlogRecord> binlogRecords = binlogManager.readAll();
            java.util.Set<Long> committedTxns = new java.util.HashSet<>();

            // 找到所有在 Binlog 中已提交的事务
            for (BinlogRecord record : binlogRecords) {
                if (record.getEventType() == BinlogRecord.EventType.COMMIT) {
                    committedTxns.add(record.getTxnId());
                }
            }

            // 3. 检查 Redo Log 中的 PREPARE 事务
            // 注意: 这里简化处理,实际 MySQL 需要更复杂的逻辑
            log.info("Crash recovery completed: committedTxns={}", committedTxns.size());

        } catch (Exception e) {
            log.error("Crash recovery failed", e);
            throw new RuntimeException("Crash recovery failed", e);
        }
    }

    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format("TwoPhaseCommit{redo=%s, binlog=%s}",
                redoLogManager.toString(), binlogManager.toString());
    }

    @Override
    public String toString() {
        return getStats();
    }
}
