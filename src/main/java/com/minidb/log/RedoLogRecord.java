package com.minidb.log;

import lombok.Data;

import java.io.Serializable;

/**
 * Redo Log 记录（RedoLogRecord）
 *
 * Redo Log 用于记录数据修改操作，支持崩溃恢复
 *
 * 核心概念：
 * 1. WAL（Write-Ahead Logging）：先写日志，后写数据
 *    - 保证持久性：即使数据页未刷盘，日志已持久化
 *    - 加速写入：顺序写日志比随机写数据页快
 *
 * 2. LSN（Log Sequence Number）：日志序列号
 *    - 全局递增，唯一标识每条日志
 *    - 用于恢复时确定日志顺序
 *
 * 3. Checkpoint：检查点
 *    - 标记已刷盘的数据页位置
 *    - 恢复时从 Checkpoint 开始，减少恢复时间
 *
 * Redo Log 的类型：
 * 1. INSERT 类型：记录插入的新数据
 * 2. DELETE 类型：记录删除操作
 * 3. UPDATE 类型：记录修改后的新值
 * 4. CHECKPOINT 类型：检查点标记
 *
 * 对应八股文知识点：
 * ✅ 什么是 WAL（Write-Ahead Logging）？
 * ✅ Redo Log 如何保证持久性？
 * ✅ LSN 是什么？有什么作用？
 * ✅ Checkpoint 机制是什么？
 *
 * @author Mini-MySQL
 */
@Data
public class RedoLogRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Redo Log 类型枚举
     */
    public enum Type {
        INSERT,     // 插入操作
        DELETE,     // 删除操作
        UPDATE,     // 更新操作
        CHECKPOINT  // 检查点
    }

    /**
     * LSN（Log Sequence Number）
     * 全局递增的日志序列号
     */
    private long lsn;

    /**
     * 关联的事务 ID
     */
    private long txnId;

    /**
     * Redo Log 类型
     */
    private Type type;

    /**
     * 表空间 ID
     */
    private int spaceId;

    /**
     * 页号
     */
    private int pageNumber;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 行 ID 或主键
     */
    private String rowId;

    /**
     * 新数据（用于 INSERT 和 UPDATE）
     * 记录修改后的值
     */
    private byte[] newData;

    /**
     * 偏移量（在页内的位置）
     */
    private int offset;

    /**
     * 数据长度
     */
    private int length;

    /**
     * 创建时间戳
     */
    private long timestamp;

    /**
     * 构造函数
     */
    public RedoLogRecord() {
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * 创建 INSERT 类型的 Redo Log
     *
     * @param txnId      事务 ID
     * @param spaceId    表空间 ID
     * @param pageNumber 页号
     * @param tableName  表名
     * @param rowId      行 ID
     * @param newData    插入的数据
     * @return Redo Log 记录
     */
    public static RedoLogRecord createInsertRedo(long txnId, int spaceId, int pageNumber,
                                                   String tableName, String rowId, byte[] newData) {
        RedoLogRecord record = new RedoLogRecord();
        record.setTxnId(txnId);
        record.setType(Type.INSERT);
        record.setSpaceId(spaceId);
        record.setPageNumber(pageNumber);
        record.setTableName(tableName);
        record.setRowId(rowId);
        record.setNewData(newData);
        return record;
    }

    /**
     * 创建 DELETE 类型的 Redo Log
     *
     * @param txnId      事务 ID
     * @param spaceId    表空间 ID
     * @param pageNumber 页号
     * @param tableName  表名
     * @param rowId      行 ID
     * @return Redo Log 记录
     */
    public static RedoLogRecord createDeleteRedo(long txnId, int spaceId, int pageNumber,
                                                   String tableName, String rowId) {
        RedoLogRecord record = new RedoLogRecord();
        record.setTxnId(txnId);
        record.setType(Type.DELETE);
        record.setSpaceId(spaceId);
        record.setPageNumber(pageNumber);
        record.setTableName(tableName);
        record.setRowId(rowId);
        return record;
    }

    /**
     * 创建 UPDATE 类型的 Redo Log
     *
     * @param txnId      事务 ID
     * @param spaceId    表空间 ID
     * @param pageNumber 页号
     * @param tableName  表名
     * @param rowId      行 ID
     * @param newData    修改后的数据
     * @return Redo Log 记录
     */
    public static RedoLogRecord createUpdateRedo(long txnId, int spaceId, int pageNumber,
                                                   String tableName, String rowId, byte[] newData) {
        RedoLogRecord record = new RedoLogRecord();
        record.setTxnId(txnId);
        record.setType(Type.UPDATE);
        record.setSpaceId(spaceId);
        record.setPageNumber(pageNumber);
        record.setTableName(tableName);
        record.setRowId(rowId);
        record.setNewData(newData);
        return record;
    }

    /**
     * 创建 CHECKPOINT 类型的 Redo Log
     *
     * @param lsn 检查点的 LSN
     * @return Redo Log 记录
     */
    public static RedoLogRecord createCheckpoint(long lsn) {
        RedoLogRecord record = new RedoLogRecord();
        record.setLsn(lsn);
        record.setType(Type.CHECKPOINT);
        return record;
    }

    @Override
    public String toString() {
        return "RedoLogRecord{" +
                "lsn=" + lsn +
                ", txnId=" + txnId +
                ", type=" + type +
                ", spaceId=" + spaceId +
                ", pageNumber=" + pageNumber +
                ", tableName='" + tableName + '\'' +
                ", rowId='" + rowId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
