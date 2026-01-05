package com.minidb.log;

import lombok.Data;

import java.io.Serializable;

/**
 * Undo Log 记录（UndoLogRecord）
 *
 * Undo Log 用于记录数据修改前的旧版本，支持事务回滚和 MVCC
 *
 * 作用：
 * 1. 事务回滚：撤销未提交的修改
 * 2. MVCC：提供历史版本用于并发读
 * 3. 崩溃恢复：配合 Redo Log 完成恢复
 *
 * Undo Log 的类型：
 * 1. INSERT 类型：记录插入的主键，回滚时删除
 * 2. DELETE 类型：记录删除的完整行，回滚时插入
 * 3. UPDATE 类型：记录修改前的旧值，回滚时恢复
 *
 * 对应八股文知识点：
 * ✅ Undo Log 的作用是什么？
 * ✅ Undo Log 如何支持 MVCC？
 * ✅ Undo Log 如何实现事务回滚？
 *
 * @author Mini-MySQL
 */
@Data
public class UndoLogRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Undo Log 类型枚举
     */
    public enum Type {
        INSERT,  // 插入操作的 Undo
        DELETE,  // 删除操作的 Undo
        UPDATE   // 更新操作的 Undo
    }

    /**
     * Undo Log ID（唯一标识）
     */
    private long undoLogId;

    /**
     * 关联的事务 ID
     */
    private long txnId;

    /**
     * Undo Log 类型
     */
    private Type type;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 行 ID 或主键
     */
    private String rowId;

    /**
     * 旧数据（用于 DELETE 和 UPDATE）
     * 简化版：使用 byte[] 存储
     * 完整版：应该是序列化后的行数据
     */
    private byte[] oldData;

    /**
     * 新数据（用于 UPDATE，可选）
     */
    private byte[] newData;

    /**
     * 指向上一条 Undo Log 的指针（形成版本链）
     * 用于 MVCC 的版本回溯
     */
    private long previousUndoLogId;

    /**
     * 创建时间戳
     */
    private long timestamp;

    /**
     * 构造函数
     */
    public UndoLogRecord() {
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * 创建 INSERT 类型的 Undo Log
     *
     * @param txnId     事务 ID
     * @param tableName 表名
     * @param rowId     行 ID
     * @return Undo Log 记录
     */
    public static UndoLogRecord createInsertUndo(long txnId, String tableName, String rowId) {
        UndoLogRecord record = new UndoLogRecord();
        record.setTxnId(txnId);
        record.setType(Type.INSERT);
        record.setTableName(tableName);
        record.setRowId(rowId);
        return record;
    }

    /**
     * 创建 DELETE 类型的 Undo Log
     *
     * @param txnId     事务 ID
     * @param tableName 表名
     * @param rowId     行 ID
     * @param oldData   删除的完整行数据
     * @return Undo Log 记录
     */
    public static UndoLogRecord createDeleteUndo(long txnId, String tableName, String rowId, byte[] oldData) {
        UndoLogRecord record = new UndoLogRecord();
        record.setTxnId(txnId);
        record.setType(Type.DELETE);
        record.setTableName(tableName);
        record.setRowId(rowId);
        record.setOldData(oldData);
        return record;
    }

    /**
     * 创建 UPDATE 类型的 Undo Log
     *
     * @param txnId     事务 ID
     * @param tableName 表名
     * @param rowId     行 ID
     * @param oldData   修改前的数据
     * @param newData   修改后的数据
     * @return Undo Log 记录
     */
    public static UndoLogRecord createUpdateUndo(long txnId, String tableName, String rowId,
                                                   byte[] oldData, byte[] newData) {
        UndoLogRecord record = new UndoLogRecord();
        record.setTxnId(txnId);
        record.setType(Type.UPDATE);
        record.setTableName(tableName);
        record.setRowId(rowId);
        record.setOldData(oldData);
        record.setNewData(newData);
        return record;
    }

    @Override
    public String toString() {
        return "UndoLogRecord{" +
                "undoLogId=" + undoLogId +
                ", txnId=" + txnId +
                ", type=" + type +
                ", tableName='" + tableName + '\'' +
                ", rowId='" + rowId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
