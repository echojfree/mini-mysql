package com.minidb.mvcc;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * ReadView（读视图）
 *
 * ReadView 是 MVCC 实现的核心，用于判断事务能看到哪些版本的数据
 *
 * ReadView 的四个核心字段：
 * 1. m_ids：创建 ReadView 时所有活跃事务的 ID 列表
 *    - 活跃事务：已开始但未提交的事务
 *    - 这些事务的修改对当前事务不可见
 *
 * 2. min_trx_id：m_ids 中的最小值
 *    - 小于这个值的事务都已提交，其修改可见
 *
 * 3. max_trx_id：创建 ReadView 时系统将要分配的下一个事务 ID
 *    - 大于等于这个值的事务还未开始，其修改不可见
 *
 * 4. creator_trx_id：创建 ReadView 的事务 ID
 *    - 自己的修改总是可见的
 *
 * 可见性判断算法：
 * 对于行记录的 DB_TRX_ID（修改该行的事务 ID），判断如下：
 * 1. 如果 DB_TRX_ID == creator_trx_id：可见（自己的修改）
 * 2. 如果 DB_TRX_ID < min_trx_id：可见（已提交的旧事务）
 * 3. 如果 DB_TRX_ID >= max_trx_id：不可见（未来的事务）
 * 4. 如果 DB_TRX_ID in m_ids：不可见（活跃事务，未提交）
 * 5. 否则：可见（已提交的事务）
 *
 * Read Committed vs Repeatable Read：
 * - RC（读已提交）：每次读取时创建新的 ReadView
 *   - 能读到其他事务新提交的数据
 *
 * - RR（可重复读）：事务开始时创建 ReadView，之后一直使用
 *   - 保证多次读取结果一致
 *   - MySQL InnoDB 的默认隔离级别
 *
 * 对应八股文知识点（最核心！）：
 * ✅ ReadView 是什么？有哪些字段？
 * ✅ MVCC 可见性判断算法
 * ✅ RC 和 RR 的 ReadView 创建时机有什么区别？
 * ✅ 为什么 RR 能防止不可重复读？
 * ✅ 快照读的实现原理
 *
 * @author Mini-MySQL
 */
@Slf4j
@Data
public class ReadView {

    /**
     * 活跃事务 ID 列表
     * 创建 ReadView 时所有未提交的事务
     */
    private List<Long> mIds;

    /**
     * 最小活跃事务 ID
     * mIds 中的最小值
     */
    private long minTrxId;

    /**
     * 下一个事务 ID
     * 创建 ReadView 时系统将要分配的下一个事务 ID
     */
    private long maxTrxId;

    /**
     * 创建者事务 ID
     * 创建这个 ReadView 的事务
     */
    private long creatorTrxId;

    /**
     * 创建时间戳
     */
    private long timestamp;

    /**
     * 构造函数
     *
     * @param mIds         活跃事务列表
     * @param minTrxId     最小活跃事务 ID
     * @param maxTrxId     下一个事务 ID
     * @param creatorTrxId 创建者事务 ID
     */
    public ReadView(List<Long> mIds, long minTrxId, long maxTrxId, long creatorTrxId) {
        this.mIds = new ArrayList<>(mIds);
        this.minTrxId = minTrxId;
        this.maxTrxId = maxTrxId;
        this.creatorTrxId = creatorTrxId;
        this.timestamp = System.currentTimeMillis();

        log.debug("Created ReadView: creator={}, min={}, max={}, activeCount={}",
                creatorTrxId, minTrxId, maxTrxId, mIds.size());
    }

    /**
     * 判断指定事务 ID 对当前 ReadView 是否可见
     *
     * 可见性算法（MVCC 核心）：
     * 1. 如果是自己的修改，可见
     * 2. 如果是已提交的旧事务，可见
     * 3. 如果是未来的事务，不可见
     * 4. 如果是当前活跃事务，不可见
     * 5. 否则，可见（已提交的事务）
     *
     * @param txnId 要判断的事务 ID
     * @return true 表示可见
     */
    public boolean isVisible(long txnId) {
        // 规则 1：自己的修改总是可见
        if (txnId == creatorTrxId) {
            log.trace("Visible: txnId={} (creator's own modification)", txnId);
            return true;
        }

        // 规则 2：小于最小活跃事务 ID，说明事务已提交，可见
        if (txnId < minTrxId) {
            log.trace("Visible: txnId={} < minTrxId={} (committed old transaction)", txnId, minTrxId);
            return true;
        }

        // 规则 3：大于等于下一个事务 ID，说明是未来的事务，不可见
        if (txnId >= maxTrxId) {
            log.trace("Not visible: txnId={} >= maxTrxId={} (future transaction)", txnId, maxTrxId);
            return false;
        }

        // 规则 4：在活跃事务列表中，说明事务未提交，不可见
        if (mIds.contains(txnId)) {
            log.trace("Not visible: txnId={} in active list (uncommitted transaction)", txnId);
            return false;
        }

        // 规则 5：否则，说明事务已提交，可见
        log.trace("Visible: txnId={} (committed transaction)", txnId);
        return true;
    }

    /**
     * 判断行记录对当前 ReadView 是否可见
     *
     * @param row 行记录
     * @return true 表示可见
     */
    public boolean isRowVisible(RowRecord row) {
        return isVisible(row.getDbTrxId());
    }

    /**
     * 获取活跃事务数量
     *
     * @return 活跃事务数量
     */
    public int getActiveTransactionCount() {
        return mIds.size();
    }

    /**
     * 打印 ReadView 详细信息
     */
    public void printDetails() {
        log.info("=== ReadView Details ===");
        log.info("Creator TxnId: {}", creatorTrxId);
        log.info("Min TxnId: {}", minTrxId);
        log.info("Max TxnId: {}", maxTrxId);
        log.info("Active Transactions: {}", mIds);
        log.info("Active Count: {}", mIds.size());
        log.info("Created At: {}", timestamp);
        log.info("=======================");
    }

    @Override
    public String toString() {
        return "ReadView{" +
                "creator=" + creatorTrxId +
                ", min=" + minTrxId +
                ", max=" + maxTrxId +
                ", activeCount=" + mIds.size() +
                '}';
    }
}
