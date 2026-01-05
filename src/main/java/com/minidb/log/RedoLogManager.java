package com.minidb.log;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Redo Log 管理器（RedoLogManager）
 *
 * 负责管理 Redo Log 的写入、刷盘和恢复
 *
 * 核心功能：
 * 1. WAL（Write-Ahead Logging）：先写日志，后写数据
 * 2. Redo Log Buffer：内存缓冲区，减少磁盘 I/O
 * 3. 循环写入：Redo Log 文件循环使用
 * 4. Checkpoint：定期检查点，加速恢复
 * 5. 崩溃恢复：使用 Redo Log 重做已提交事务
 *
 * Redo Log 的写入流程：
 * 1. 生成 Redo Log 记录，分配 LSN
 * 2. 写入 Redo Log Buffer（内存）
 * 3. 定期或事务提交时刷盘（fsync）
 * 4. 返回成功
 *
 * Redo Log 的刷盘策略：
 * 1. 事务提交时刷盘（保证持久性）
 * 2. Buffer 满时刷盘
 * 3. 定期刷盘（后台线程）
 *
 * Checkpoint 机制：
 * - 记录已刷盘的最大 LSN
 * - 恢复时从 Checkpoint 开始，避免重做已刷盘的数据
 *
 * 对应八股文知识点：
 * ✅ WAL 机制的实现原理
 * ✅ Redo Log Buffer 的作用
 * ✅ Redo Log 的刷盘策略
 * ✅ Checkpoint 如何加速恢复
 * ✅ 如何使用 Redo Log 进行崩溃恢复
 *
 * @author Mini-MySQL
 */
@Slf4j
@Getter
public class RedoLogManager {

    /**
     * LSN（Log Sequence Number）生成器
     */
    private final AtomicLong lsnGenerator;

    /**
     * Redo Log Buffer（内存缓冲区）
     * 简化版：使用 List 存储
     * 完整版：使用循环缓冲区
     */
    private final List<RedoLogRecord> redoLogBuffer;

    /**
     * Redo Log 文件路径
     */
    private final String redoLogFilePath;

    /**
     * Buffer 最大大小（条数）
     */
    private final int bufferMaxSize;

    /**
     * 最后一次 Checkpoint 的 LSN
     */
    private volatile long checkpointLsn;

    /**
     * 已刷盘的最大 LSN
     */
    private volatile long flushedLsn;

    /**
     * 读写锁（保护 Buffer 操作）
     */
    private final ReentrantReadWriteLock lock;

    /**
     * 统计信息：刷盘次数
     */
    private long flushCount;

    /**
     * 统计信息：Checkpoint 次数
     */
    private long checkpointCount;

    /**
     * 构造函数
     *
     * @param redoLogFilePath Redo Log 文件路径
     * @param bufferMaxSize   Buffer 最大大小
     */
    public RedoLogManager(String redoLogFilePath, int bufferMaxSize) {
        this.lsnGenerator = new AtomicLong(1);
        this.redoLogBuffer = new ArrayList<>();
        this.redoLogFilePath = redoLogFilePath;
        this.bufferMaxSize = bufferMaxSize;
        this.checkpointLsn = 0;
        this.flushedLsn = 0;
        this.lock = new ReentrantReadWriteLock();
        this.flushCount = 0;
        this.checkpointCount = 0;

        log.info("RedoLogManager initialized: filePath={}, bufferSize={}", redoLogFilePath, bufferMaxSize);
    }

    /**
     * 默认构造函数
     */
    public RedoLogManager() {
        this("redo.log", 1000);
    }

    /**
     * 记录 INSERT 操作的 Redo Log
     *
     * @param txnId      事务 ID
     * @param spaceId    表空间 ID
     * @param pageNumber 页号
     * @param tableName  表名
     * @param rowId      行 ID
     * @param newData    插入的数据
     * @return LSN
     */
    public long logInsert(long txnId, int spaceId, int pageNumber,
                          String tableName, String rowId, byte[] newData) {
        RedoLogRecord record = RedoLogRecord.createInsertRedo(txnId, spaceId, pageNumber,
                tableName, rowId, newData);
        return appendLog(record);
    }

    /**
     * 记录 DELETE 操作的 Redo Log
     *
     * @param txnId      事务 ID
     * @param spaceId    表空间 ID
     * @param pageNumber 页号
     * @param tableName  表名
     * @param rowId      行 ID
     * @return LSN
     */
    public long logDelete(long txnId, int spaceId, int pageNumber,
                          String tableName, String rowId) {
        RedoLogRecord record = RedoLogRecord.createDeleteRedo(txnId, spaceId, pageNumber,
                tableName, rowId);
        return appendLog(record);
    }

    /**
     * 记录 UPDATE 操作的 Redo Log
     *
     * @param txnId      事务 ID
     * @param spaceId    表空间 ID
     * @param pageNumber 页号
     * @param tableName  表名
     * @param rowId      行 ID
     * @param newData    修改后的数据
     * @return LSN
     */
    public long logUpdate(long txnId, int spaceId, int pageNumber,
                          String tableName, String rowId, byte[] newData) {
        RedoLogRecord record = RedoLogRecord.createUpdateRedo(txnId, spaceId, pageNumber,
                tableName, rowId, newData);
        return appendLog(record);
    }

    /**
     * 添加 Redo Log 记录到 Buffer
     *
     * @param record Redo Log 记录
     * @return LSN
     */
    private long appendLog(RedoLogRecord record) {
        lock.writeLock().lock();
        try {
            // 分配 LSN
            long lsn = lsnGenerator.getAndIncrement();
            record.setLsn(lsn);

            // 写入 Buffer
            redoLogBuffer.add(record);

            log.debug("Appended Redo Log: {}", record);

            // 如果 Buffer 满了，触发刷盘
            if (redoLogBuffer.size() >= bufferMaxSize) {
                flushInternal();
            }

            return lsn;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 刷盘（Flush）
     * 将 Redo Log Buffer 写入磁盘
     */
    public void flush() {
        lock.writeLock().lock();
        try {
            flushInternal();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 内部刷盘方法（需要持有写锁）
     */
    private void flushInternal() {
        if (redoLogBuffer.isEmpty()) {
            return;
        }

        try {
            File file = new File(redoLogFilePath);
            boolean fileExists = file.exists() && file.length() > 0;

            // 追加写入文件
            FileOutputStream fos = new FileOutputStream(redoLogFilePath, true);
            ObjectOutputStream oos;

            if (fileExists) {
                // 文件已存在，使用自定义 ObjectOutputStream 避免写入头部
                oos = new ObjectOutputStream(fos) {
                    @Override
                    protected void writeStreamHeader() throws IOException {
                        // 不写入流头部
                        reset();
                    }
                };
            } else {
                // 文件不存在，使用普通 ObjectOutputStream
                oos = new ObjectOutputStream(fos);
            }

            try {
                for (RedoLogRecord record : redoLogBuffer) {
                    oos.writeObject(record);
                    flushedLsn = Math.max(flushedLsn, record.getLsn());
                }

                // 强制刷盘（fsync）
                fos.getFD().sync();
            } finally {
                oos.close();
                fos.close();
            }

            flushCount++;
            log.info("Flushed Redo Log Buffer: count={}, flushedLsn={}", redoLogBuffer.size(), flushedLsn);

            // 清空 Buffer
            redoLogBuffer.clear();

        } catch (IOException e) {
            log.error("Failed to flush Redo Log", e);
            throw new RuntimeException("Flush failed", e);
        }
    }

    /**
     * 创建 Checkpoint
     * 记录当前已刷盘的最大 LSN
     */
    public void checkpoint() {
        lock.writeLock().lock();
        try {
            // 先刷盘
            flushInternal();

            // 记录 Checkpoint LSN
            checkpointLsn = flushedLsn;
            checkpointCount++;

            // 写入 Checkpoint 记录
            RedoLogRecord checkpointRecord = RedoLogRecord.createCheckpoint(checkpointLsn);
            redoLogBuffer.add(checkpointRecord);
            flushInternal();

            log.info("Created Checkpoint: checkpointLsn={}, count={}", checkpointLsn, checkpointCount);

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 崩溃恢复（Crash Recovery）
     * 从 Redo Log 恢复未刷盘的数据
     *
     * @return 恢复的事务数量
     */
    public int recover() {
        File file = new File(redoLogFilePath);
        if (!file.exists()) {
            log.info("No Redo Log file found, skip recovery");
            return 0;
        }

        log.info("Starting crash recovery from Redo Log: {}", redoLogFilePath);

        lock.writeLock().lock();
        try {
            List<RedoLogRecord> redoLogs = new ArrayList<>();
            long recoveryStartLsn = checkpointLsn;

            // 读取 Redo Log 文件
            try (FileInputStream fis = new FileInputStream(redoLogFilePath);
                 ObjectInputStream ois = new ObjectInputStream(fis)) {

                while (true) {
                    try {
                        RedoLogRecord record = (RedoLogRecord) ois.readObject();

                        // 更新 Checkpoint LSN
                        if (record.getType() == RedoLogRecord.Type.CHECKPOINT) {
                            recoveryStartLsn = record.getLsn();
                            checkpointLsn = record.getLsn();
                            continue;
                        }

                        // 只恢复 Checkpoint 之后的日志
                        if (record.getLsn() > recoveryStartLsn) {
                            redoLogs.add(record);
                        }

                    } catch (EOFException e) {
                        // 文件读取完毕
                        break;
                    }
                }

            } catch (IOException | ClassNotFoundException e) {
                log.error("Failed to read Redo Log file", e);
                throw new RuntimeException("Recovery failed", e);
            }

            // 重做操作
            for (RedoLogRecord record : redoLogs) {
                applyRedo(record);
            }

            log.info("Crash recovery completed: recoveredLogs={}, startLsn={}", redoLogs.size(), recoveryStartLsn);
            return redoLogs.size();

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 应用单条 Redo Log（执行重做操作）
     *
     * @param record Redo Log 记录
     */
    private void applyRedo(RedoLogRecord record) {
        switch (record.getType()) {
            case INSERT:
                log.debug("Redo INSERT: table={}, rowId={}", record.getTableName(), record.getRowId());
                // 实际应该调用存储引擎插入数据
                break;

            case DELETE:
                log.debug("Redo DELETE: table={}, rowId={}", record.getTableName(), record.getRowId());
                // 实际应该调用存储引擎删除数据
                break;

            case UPDATE:
                log.debug("Redo UPDATE: table={}, rowId={}", record.getTableName(), record.getRowId());
                // 实际应该调用存储引擎更新数据
                break;

            case CHECKPOINT:
                // Checkpoint 记录不需要重做
                break;
        }
    }

    /**
     * 获取 Buffer 中的日志数量
     *
     * @return 日志数量
     */
    public int getBufferSize() {
        lock.readLock().lock();
        try {
            return redoLogBuffer.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 打印统计信息
     */
    public void printStats() {
        log.info("=== Redo Log Manager Statistics ===");
        log.info("Current LSN: {}", lsnGenerator.get());
        log.info("Flushed LSN: {}", flushedLsn);
        log.info("Checkpoint LSN: {}", checkpointLsn);
        log.info("Buffer Size: {}", getBufferSize());
        log.info("Flush Count: {}", flushCount);
        log.info("Checkpoint Count: {}", checkpointCount);
        log.info("===================================");
    }

    /**
     * 清理 Redo Log 文件
     */
    public void clear() {
        File file = new File(redoLogFilePath);
        if (file.exists()) {
            file.delete();
            log.info("Cleared Redo Log file: {}", redoLogFilePath);
        }
    }
}
