package com.minidb.log;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Binlog 管理器
 *
 * 功能:
 * 1. 写入 Binlog 记录
 * 2. 读取 Binlog 用于恢复
 * 3. Binlog 文件管理
 * 4. 与 Redo Log 协同工作(两阶段提交)
 *
 * 工作流程:
 * 1. 事务执行时记录 SQL 语句
 * 2. 事务提交时写入 Binlog
 * 3. 配合 Redo Log 实现两阶段提交
 *
 * Binlog 文件格式:
 * - 文件名: binlog.000001, binlog.000002, ...
 * - 每条记录前缀: [长度(4字节)][数据]
 * - 按序写入,循环复用
 *
 * 对应八股文知识点:
 * ✅ Binlog 的作用
 * ✅ Binlog 写入流程
 * ✅ 两阶段提交中 Binlog 的角色
 *
 * @author Mini-MySQL
 */
@Slf4j
@Getter
public class BinlogManager {

    /**
     * Binlog 文件路径
     */
    private final String binlogFilePath;

    /**
     * 文件通道
     */
    private FileChannel fileChannel;

    /**
     * RandomAccessFile
     */
    private RandomAccessFile randomAccessFile;

    /**
     * 当前 LSN (Log Sequence Number)
     */
    private final AtomicLong currentLsn;

    /**
     * 锁 (保证写入顺序)
     */
    private final ReentrantLock lock;

    /**
     * Binlog 记录数
     */
    private long recordCount;

    /**
     * 构造函数
     *
     * @param binlogFilePath Binlog 文件路径
     */
    public BinlogManager(String binlogFilePath) throws IOException {
        this.binlogFilePath = binlogFilePath;
        this.currentLsn = new AtomicLong(0);
        this.lock = new ReentrantLock();
        this.recordCount = 0;

        // 打开或创建 Binlog 文件
        File file = new File(binlogFilePath);
        boolean isNew = !file.exists();

        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.fileChannel = randomAccessFile.getChannel();

        if (isNew) {
            log.info("Created new binlog file: {}", binlogFilePath);
        } else {
            // 恢复 LSN
            recoverLsn();
            log.info("Opened existing binlog file: {}, recordCount={}, lsn={}",
                    binlogFilePath, recordCount, currentLsn.get());
        }
    }

    /**
     * 写入 Binlog 记录
     *
     * @param record Binlog 记录
     * @return 分配的 LSN
     */
    public long append(BinlogRecord record) throws IOException {
        lock.lock();
        try {
            // 1. 分配 LSN
            long lsn = currentLsn.incrementAndGet();
            record.setLsn(lsn);

            // 2. 序列化记录
            byte[] data = record.serialize();

            // 3. 写入长度前缀
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            lengthBuffer.putInt(data.length);
            lengthBuffer.flip();

            // 4. 定位到文件末尾
            fileChannel.position(fileChannel.size());

            // 5. 写入长度和数据
            fileChannel.write(lengthBuffer);
            fileChannel.write(ByteBuffer.wrap(data));

            // 6. 更新统计信息
            recordCount++;

            log.debug("Appended binlog: lsn={}, txnId={}, type={}, table={}",
                    lsn, record.getTxnId(), record.getEventType(), record.getTableName());

            return lsn;

        } finally {
            lock.unlock();
        }
    }

    /**
     * 刷盘 (强制写入磁盘)
     */
    public void flush() throws IOException {
        lock.lock();
        try {
            fileChannel.force(true);
            log.debug("Flushed binlog to disk: recordCount={}", recordCount);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 读取所有 Binlog 记录
     *
     * @return Binlog 记录列表
     */
    public List<BinlogRecord> readAll() throws IOException {
        List<BinlogRecord> records = new ArrayList<>();

        lock.lock();
        try {
            // 定位到文件开头
            fileChannel.position(0);

            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);

            while (fileChannel.position() < fileChannel.size()) {
                // 读取长度
                lengthBuffer.clear();
                int bytesRead = fileChannel.read(lengthBuffer);
                if (bytesRead < 4) {
                    break; // 文件结束
                }

                lengthBuffer.flip();
                int dataLength = lengthBuffer.getInt();

                // 读取数据
                ByteBuffer dataBuffer = ByteBuffer.allocate(dataLength);
                fileChannel.read(dataBuffer);
                dataBuffer.flip();

                // 反序列化
                byte[] data = new byte[dataLength];
                dataBuffer.get(data);
                BinlogRecord record = BinlogRecord.deserialize(data);

                records.add(record);
            }

            log.debug("Read {} binlog records", records.size());
            return records;

        } finally {
            lock.unlock();
        }
    }

    /**
     * 读取指定事务的 Binlog 记录
     *
     * @param txnId 事务 ID
     * @return Binlog 记录列表
     */
    public List<BinlogRecord> readByTransaction(long txnId) throws IOException {
        List<BinlogRecord> allRecords = readAll();
        List<BinlogRecord> txnRecords = new ArrayList<>();

        for (BinlogRecord record : allRecords) {
            if (record.getTxnId() == txnId) {
                txnRecords.add(record);
            }
        }

        return txnRecords;
    }

    /**
     * 关闭 Binlog 管理器
     */
    public void close() throws IOException {
        lock.lock();
        try {
            if (fileChannel != null) {
                fileChannel.close();
            }
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
            log.info("Closed binlog: recordCount={}", recordCount);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 清空 Binlog (用于测试)
     */
    public void clear() throws IOException {
        lock.lock();
        try {
            fileChannel.truncate(0);
            currentLsn.set(0);
            recordCount = 0;
            log.info("Cleared binlog");
        } finally {
            lock.unlock();
        }
    }

    /**
     * 恢复 LSN (从文件中读取最大 LSN)
     */
    private void recoverLsn() throws IOException {
        List<BinlogRecord> records = readAll();
        recordCount = records.size();

        if (!records.isEmpty()) {
            BinlogRecord lastRecord = records.get(records.size() - 1);
            currentLsn.set(lastRecord.getLsn());
        }
    }

    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format("Binlog{file=%s, records=%d, lsn=%d}",
                binlogFilePath, recordCount, currentLsn.get());
    }

    @Override
    public String toString() {
        return getStats();
    }
}
