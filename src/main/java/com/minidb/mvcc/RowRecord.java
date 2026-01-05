package com.minidb.mvcc;

import lombok.Data;

/**
 * 行记录（RowRecord）
 *
 * InnoDB 的每行记录都包含隐藏字段，用于实现 MVCC
 *
 * 隐藏字段：
 * 1. DB_TRX_ID（6 字节）：最后修改该行的事务 ID
 *    - 记录最近一次修改这行数据的事务
 *    - 用于 MVCC 可见性判断
 *
 * 2. DB_ROLL_PTR（7 字节）：回滚指针
 *    - 指向 Undo Log 中该行的旧版本
 *    - 构成版本链，支持 MVCC 读取历史版本
 *
 * 3. DB_ROW_ID（6 字节）：隐藏主键
 *    - 如果表没有定义主键，InnoDB 会自动创建
 *    - 如果有主键，则不需要这个字段
 *
 * MVCC 原理：
 * - 每次修改行时，都会记录当前事务 ID（DB_TRX_ID）
 * - 旧版本通过 DB_ROLL_PTR 存储在 Undo Log 中
 * - 多个版本形成版本链
 * - 查询时根据 ReadView 判断哪个版本可见
 *
 * 对应八股文知识点：
 * ✅ InnoDB 行记录的隐藏字段有哪些？
 * ✅ DB_TRX_ID 和 DB_ROLL_PTR 的作用
 * ✅ MVCC 如何通过版本链实现并发控制
 *
 * @author Mini-MySQL
 */
@Data
public class RowRecord {

    /**
     * 行 ID（主键）
     */
    private String rowId;

    /**
     * 用户数据
     * 简化版：使用 byte[] 存储
     * 完整版：应该是结构化的列数据
     */
    private byte[] data;

    /**
     * DB_TRX_ID：最后修改该行的事务 ID
     * InnoDB 中为 6 字节（48 位）
     */
    private long dbTrxId;

    /**
     * DB_ROLL_PTR：回滚指针，指向 Undo Log 中的旧版本
     * InnoDB 中为 7 字节（56 位）
     * 这里简化为 long，存储 Undo Log ID
     */
    private long dbRollPtr;

    /**
     * DB_ROW_ID：隐藏主键（如果表没有定义主键）
     * InnoDB 中为 6 字节（48 位）
     * 简化版：直接使用 rowId 字符串
     */

    /**
     * 构造函数
     *
     * @param rowId 行 ID
     * @param data  数据
     */
    public RowRecord(String rowId, byte[] data) {
        this.rowId = rowId;
        this.data = data;
        this.dbTrxId = 0;
        this.dbRollPtr = 0;
    }

    /**
     * 更新行数据（由事务修改）
     *
     * @param txnId      修改该行的事务 ID
     * @param newData    新数据
     * @param undoLogId  指向旧版本的 Undo Log ID
     */
    public void update(long txnId, byte[] newData, long undoLogId) {
        this.dbTrxId = txnId;
        this.data = newData;
        this.dbRollPtr = undoLogId;
    }

    /**
     * 插入行数据（由事务插入）
     *
     * @param txnId 插入该行的事务 ID
     */
    public void insert(long txnId) {
        this.dbTrxId = txnId;
        this.dbRollPtr = 0; // 插入操作没有旧版本
    }

    /**
     * 获取版本信息摘要
     *
     * @return 版本信息字符串
     */
    public String getVersionInfo() {
        return String.format("RowRecord{rowId='%s', dbTrxId=%d, dbRollPtr=%d}",
                rowId, dbTrxId, dbRollPtr);
    }

    @Override
    public String toString() {
        return "RowRecord{" +
                "rowId='" + rowId + '\'' +
                ", dbTrxId=" + dbTrxId +
                ", dbRollPtr=" + dbRollPtr +
                ", dataLength=" + (data != null ? data.length : 0) +
                '}';
    }
}
