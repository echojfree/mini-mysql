package com.minidb.log;

import lombok.Data;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Binlog 日志记录
 *
 * 核心概念:
 * 1. Binlog 是 MySQL 的逻辑日志,记录所有修改数据的 SQL 语句
 * 2. 用于主从复制、数据恢复
 * 3. 与 Redo Log 的区别:
 *    - Redo Log 是物理日志(记录页的物理修改)
 *    - Binlog 是逻辑日志(记录 SQL 语句)
 *    - Redo Log 是 InnoDB 特有,Binlog 是 MySQL Server 层的
 *
 * Binlog 的三种格式:
 * 1. STATEMENT: 记录 SQL 语句 (体积小,但可能不一致)
 * 2. ROW: 记录行的变化 (体积大,但数据一致)
 * 3. MIXED: 混合模式 (自动选择)
 *
 * 本实现采用简化的 STATEMENT 格式
 *
 * 对应八股文知识点:
 * ✅ Redo Log vs Binlog 区别
 * ✅ Binlog 的三种格式
 * ✅ 主从复制原理
 *
 * @author Mini-MySQL
 */
@Data
public class BinlogRecord {

    /**
     * Binlog 事件类型
     */
    public enum EventType {
        INSERT,      // INSERT 语句
        UPDATE,      // UPDATE 语句
        DELETE,      // DELETE 语句
        BEGIN,       // 事务开始
        COMMIT,      // 事务提交
        ROLLBACK     // 事务回滚
    }

    /**
     * 事务 ID
     */
    private final long txnId;

    /**
     * 事件类型
     */
    private final EventType eventType;

    /**
     * 表名
     */
    private final String tableName;

    /**
     * SQL 语句 (STATEMENT 格式)
     */
    private final String sqlStatement;

    /**
     * 时间戳
     */
    private final long timestamp;

    /**
     * LSN (Log Sequence Number)
     * 与 Redo Log 的 LSN 对应,用于两阶段提交
     */
    private long lsn;

    /**
     * 构造函数
     */
    public BinlogRecord(long txnId, EventType eventType, String tableName, String sqlStatement) {
        this.txnId = txnId;
        this.eventType = eventType;
        this.tableName = tableName;
        this.sqlStatement = sqlStatement;
        this.timestamp = System.currentTimeMillis();
        this.lsn = 0; // 稍后由 BinlogManager 分配
    }

    /**
     * 序列化为字节数组
     *
     * 格式:
     * [txnId(8)] [eventType(4)] [timestamp(8)] [lsn(8)]
     * [tableNameLen(4)] [tableName(N)] [sqlLen(4)] [sql(N)]
     */
    public byte[] serialize() {
        byte[] tableNameBytes = tableName == null ? new byte[0] : tableName.getBytes(StandardCharsets.UTF_8);
        byte[] sqlBytes = sqlStatement == null ? new byte[0] : sqlStatement.getBytes(StandardCharsets.UTF_8);

        int totalSize = 8 + 4 + 8 + 8 + 4 + tableNameBytes.length + 4 + sqlBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.putLong(txnId);
        buffer.putInt(eventType.ordinal());
        buffer.putLong(timestamp);
        buffer.putLong(lsn);

        buffer.putInt(tableNameBytes.length);
        buffer.put(tableNameBytes);

        buffer.putInt(sqlBytes.length);
        buffer.put(sqlBytes);

        return buffer.array();
    }

    /**
     * 从字节数组反序列化
     */
    public static BinlogRecord deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        long txnId = buffer.getLong();
        EventType eventType = EventType.values()[buffer.getInt()];
        long timestamp = buffer.getLong();
        long lsn = buffer.getLong();

        int tableNameLen = buffer.getInt();
        byte[] tableNameBytes = new byte[tableNameLen];
        buffer.get(tableNameBytes);
        String tableName = new String(tableNameBytes, StandardCharsets.UTF_8);

        int sqlLen = buffer.getInt();
        byte[] sqlBytes = new byte[sqlLen];
        buffer.get(sqlBytes);
        String sqlStatement = new String(sqlBytes, StandardCharsets.UTF_8);

        BinlogRecord record = new BinlogRecord(txnId, eventType, tableName, sqlStatement);
        record.setLsn(lsn);

        // 恢复时间戳
        try {
            java.lang.reflect.Field timestampField = BinlogRecord.class.getDeclaredField("timestamp");
            timestampField.setAccessible(true);
            timestampField.set(record, timestamp);
        } catch (Exception e) {
            // 如果无法设置,使用当前时间
        }

        return record;
    }

    /**
     * 获取日志大小
     */
    public int getSize() {
        return serialize().length;
    }

    @Override
    public String toString() {
        return String.format("BinlogRecord{txnId=%d, type=%s, table=%s, lsn=%d, sql=%s}",
                txnId, eventType, tableName, lsn, sqlStatement);
    }
}
