package com.minidb.storage.file;

import com.minidb.storage.page.Page;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.minidb.common.Constants.*;

/**
 * 表空间（TableSpace）
 *
 * 表空间是 InnoDB 存储引擎管理数据的逻辑单元，一个表空间对应一个物理文件（.ibd）
 *
 * 核心功能：
 * 1. 页面的持久化（写入磁盘）
 * 2. 页面的加载（从磁盘读取）
 * 3. 文件空间管理（分配新页面）
 * 4. 文件锁管理（防止并发冲突）
 *
 * 文件结构：
 * - 文件由固定大小的页面组成（每页 16KB）
 * - 页号从 0 开始递增
 * - 页面 0 通常是文件头（存储元数据）
 *
 * 对应八股文知识点：
 * ✅ 什么是表空间？
 * ✅ InnoDB 的文件结构
 * ✅ 页面如何存储在磁盘上？
 * ✅ 随机访问文件（RandomAccessFile）的使用
 *
 * @author Mini-MySQL
 */
@Slf4j
@Getter
public class TableSpace {

    /**
     * 表空间 ID
     */
    private final int spaceId;

    /**
     * 表空间名称（通常是表名）
     */
    private final String spaceName;

    /**
     * 数据文件路径
     */
    private final String filePath;

    /**
     * 数据文件对象
     */
    private final File dataFile;

    /**
     * 随机访问文件对象
     * RandomAccessFile 支持在文件的任意位置读写，非常适合页式存储
     */
    private RandomAccessFile randomAccessFile;

    /**
     * 文件通道
     * 用于文件锁和高性能 I/O 操作
     */
    private FileChannel fileChannel;

    /**
     * 文件锁
     * 防止多个进程同时访问同一个表空间文件
     */
    private FileLock fileLock;

    /**
     * 当前分配的最大页号
     * 用于分配新页面
     */
    private int maxPageNumber;

    /**
     * 页号到文件偏移量的映射
     * 计算公式：offset = pageNumber * PAGE_SIZE
     */
    private final ConcurrentHashMap<Integer, Long> pageOffsetMap;

    /**
     * 读写锁
     * 保护表空间的并发访问
     */
    private final ReadWriteLock rwLock;

    /**
     * 表空间是否已打开
     */
    private volatile boolean opened;

    /**
     * 构造函数
     *
     * @param spaceId   表空间 ID
     * @param spaceName 表空间名称
     * @param dataDir   数据目录
     */
    public TableSpace(int spaceId, String spaceName, String dataDir) {
        this.spaceId = spaceId;
        this.spaceName = spaceName;
        this.filePath = dataDir + File.separator + spaceName + DATA_FILE_SUFFIX;
        this.dataFile = new File(filePath);
        this.pageOffsetMap = new ConcurrentHashMap<>();
        this.rwLock = new ReentrantReadWriteLock();
        this.maxPageNumber = -1;
        this.opened = false;
    }

    /**
     * 打开表空间
     * 如果文件不存在则创建新文件
     *
     * @throws IOException 文件操作异常
     */
    public void open() throws IOException {
        rwLock.writeLock().lock();
        try {
            if (opened) {
                log.warn("TableSpace {} is already opened", spaceName);
                return;
            }

            // 确保数据目录存在
            File parentDir = dataFile.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                if (!parentDir.mkdirs()) {
                    throw new IOException("Failed to create data directory: " + parentDir);
                }
            }

            // 打开文件（读写模式）
            randomAccessFile = new RandomAccessFile(dataFile, "rw");
            fileChannel = randomAccessFile.getChannel();

            // 获取排他文件锁（防止多进程访问）
            try {
                fileLock = fileChannel.tryLock();
                if (fileLock == null) {
                    throw new IOException("Failed to acquire file lock on " + filePath);
                }
            } catch (Exception e) {
                throw new IOException("File is locked by another process: " + filePath, e);
            }

            // 计算当前最大页号
            long fileSize = randomAccessFile.length();
            if (fileSize > 0) {
                maxPageNumber = (int) (fileSize / PAGE_SIZE) - 1;
                log.info("Opened existing TableSpace {}: {} pages, size={} bytes",
                        spaceName, maxPageNumber + 1, fileSize);
            } else {
                maxPageNumber = -1;
                log.info("Created new TableSpace {}", spaceName);
            }

            opened = true;

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * 关闭表空间
     * 释放文件锁和文件句柄
     *
     * @throws IOException 文件操作异常
     */
    public void close() throws IOException {
        rwLock.writeLock().lock();
        try {
            if (!opened) {
                return;
            }

            // 释放文件锁
            if (fileLock != null && fileLock.isValid()) {
                fileLock.release();
                fileLock = null;
            }

            // 关闭文件通道
            if (fileChannel != null && fileChannel.isOpen()) {
                fileChannel.close();
                fileChannel = null;
            }

            // 关闭随机访问文件
            if (randomAccessFile != null) {
                randomAccessFile.close();
                randomAccessFile = null;
            }

            opened = false;
            log.info("Closed TableSpace {}", spaceName);

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * 分配一个新页面
     *
     * @return 新页面的页号
     * @throws IOException 文件操作异常
     */
    public int allocatePage() throws IOException {
        rwLock.writeLock().lock();
        try {
            checkOpened();

            // 分配新页号
            int newPageNumber = ++maxPageNumber;

            // 扩展文件大小
            long newFileSize = (long) (newPageNumber + 1) * PAGE_SIZE;
            randomAccessFile.setLength(newFileSize);

            log.debug("Allocated new page: spaceId={}, pageNumber={}", spaceId, newPageNumber);

            return newPageNumber;

        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * 写入页面到磁盘
     *
     * @param page 要写入的页面
     * @throws IOException 文件操作异常
     */
    public void writePage(Page page) throws IOException {
        rwLock.readLock().lock();
        try {
            checkOpened();

            int pageNumber = page.getHeader().getPageNumber();

            // 检查页号有效性
            if (pageNumber < 0) {
                throw new IllegalArgumentException("Invalid page number: " + pageNumber);
            }

            // 如果页号超出当前范围，需要扩展文件
            if (pageNumber > maxPageNumber) {
                rwLock.readLock().unlock();
                rwLock.writeLock().lock();
                try {
                    // 双重检查
                    if (pageNumber > maxPageNumber) {
                        long newFileSize = (long) (pageNumber + 1) * PAGE_SIZE;
                        randomAccessFile.setLength(newFileSize);
                        maxPageNumber = pageNumber;
                    }
                    rwLock.readLock().lock();
                } finally {
                    rwLock.writeLock().unlock();
                }
            }

            // 计算文件偏移量
            long offset = (long) pageNumber * PAGE_SIZE;

            // 序列化页面
            byte[] pageBytes = page.serialize();

            // 定位到指定位置并写入
            randomAccessFile.seek(offset);
            randomAccessFile.write(pageBytes);

            // 强制刷盘（确保数据真正写入磁盘）
            // 在生产环境中，可以根据配置决定是否立即刷盘
            fileChannel.force(false);

            // 标记页面为干净（已刷盘）
            page.setDirty(false);

            log.debug("Wrote page to disk: spaceId={}, pageNumber={}, offset={}",
                    spaceId, pageNumber, offset);

        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * 从磁盘读取页面
     *
     * @param pageNumber 页号
     * @return 读取的页面
     * @throws IOException 文件操作异常
     */
    public Page readPage(int pageNumber) throws IOException {
        rwLock.readLock().lock();
        try {
            checkOpened();

            // 检查页号有效性
            if (pageNumber < 0 || pageNumber > maxPageNumber) {
                throw new IllegalArgumentException(
                    String.format("Invalid page number: %d (max: %d)", pageNumber, maxPageNumber));
            }

            // 计算文件偏移量
            long offset = (long) pageNumber * PAGE_SIZE;

            // 读取页面数据
            byte[] pageBytes = new byte[PAGE_SIZE];
            randomAccessFile.seek(offset);
            int bytesRead = randomAccessFile.read(pageBytes);

            if (bytesRead != PAGE_SIZE) {
                throw new IOException(
                    String.format("Failed to read complete page: expected %d bytes, got %d",
                        PAGE_SIZE, bytesRead));
            }

            // 反序列化页面
            Page page = Page.deserialize(pageBytes);

            // 验证页面完整性
            if (!page.verify()) {
                log.error("Page verification failed: spaceId={}, pageNumber={}", spaceId, pageNumber);
                throw new IOException("Page data corrupted: " + pageNumber);
            }

            log.debug("Read page from disk: spaceId={}, pageNumber={}, offset={}",
                    spaceId, pageNumber, offset);

            return page;

        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * 同步磁盘数据
     * 确保所有缓冲的数据都写入磁盘
     *
     * @throws IOException 文件操作异常
     */
    public void sync() throws IOException {
        rwLock.readLock().lock();
        try {
            checkOpened();
            fileChannel.force(true);
            log.debug("Synced TableSpace {} to disk", spaceName);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * 获取表空间的页面数量
     *
     * @return 页面数量
     */
    public int getPageCount() {
        return maxPageNumber + 1;
    }

    /**
     * 获取表空间文件大小
     *
     * @return 文件大小（字节）
     * @throws IOException 文件操作异常
     */
    public long getFileSize() throws IOException {
        rwLock.readLock().lock();
        try {
            checkOpened();
            return randomAccessFile.length();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * 检查表空间是否已打开
     *
     * @throws IllegalStateException 表空间未打开
     */
    private void checkOpened() {
        if (!opened) {
            throw new IllegalStateException("TableSpace is not opened: " + spaceName);
        }
    }

    /**
     * 删除表空间文件
     * 警告：此操作不可逆！
     *
     * @return 是否删除成功
     */
    public boolean delete() {
        rwLock.writeLock().lock();
        try {
            // 先关闭文件
            if (opened) {
                try {
                    close();
                } catch (IOException e) {
                    log.error("Failed to close TableSpace before deletion", e);
                    return false;
                }
            }

            // 删除文件
            if (dataFile.exists()) {
                boolean deleted = dataFile.delete();
                if (deleted) {
                    log.info("Deleted TableSpace file: {}", filePath);
                } else {
                    log.error("Failed to delete TableSpace file: {}", filePath);
                }
                return deleted;
            }

            return true;

        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
