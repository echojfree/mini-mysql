package com.minidb.storage.file;

import com.minidb.storage.page.Page;
import com.minidb.storage.page.PageType;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * TableSpace (表空间)
 *
 * MySQL InnoDB 表空间文件 (.ibd):
 * - 包含多个页面 (Page)
 * - 页号从 0 开始连续编号
 * - 页号 * PAGE_SIZE = 文件偏移量
 *
 * 功能:
 * 1. 分配新页面
 * 2. 读取指定页面
 * 3. 写入指定页面
 * 4. 持久化到磁盘
 *
 * 对应八股文知识点:
 * ✅ 表空间的概念
 * ✅ .ibd 文件结构
 * ✅ RandomAccessFile 的使用
 * ✅ 页号到文件偏移量的映射
 *
 * @author Mini-MySQL
 */
@Slf4j
public class TableSpace {

    /**
     * 表空间 ID
     */
    private final int spaceId;

    /**
     * 表空间文件
     */
    private final File file;

    /**
     * 随机访问文件
     */
    private final RandomAccessFile raf;

    /**
     * 文件通道 (用于高效 I/O)
     */
    private final FileChannel channel;

    /**
     * 下一个可用页号
     */
    private int nextPageNo;

    /**
     * 读写锁 (保证线程安全)
     */
    private final ReadWriteLock lock;

    /**
     * 构造函数
     *
     * @param spaceId 表空间 ID
     * @param filePath 文件路径
     * @throws IOException IO 异常
     */
    public TableSpace(int spaceId, String filePath) throws IOException {
        this.spaceId = spaceId;
        this.file = new File(filePath);
        this.lock = new ReentrantReadWriteLock();

        // 创建父目录
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        // 打开文件 (读写模式)
        this.raf = new RandomAccessFile(file, "rw");
        this.channel = raf.getChannel();

        // 计算下一个可用页号
        long fileSize = raf.length();
        this.nextPageNo = (int) (fileSize / Page.PAGE_SIZE);

        log.info("Opened tablespace: spaceId={}, file={}, size={} bytes, pages={}",
                spaceId, filePath, fileSize, nextPageNo);
    }

    /**
     * 分配新页面
     *
     * @param pageType 页类型
     * @return 新页面
     */
    public Page allocatePage(PageType pageType) {
        lock.writeLock().lock();
        try {
            int pageNo = nextPageNo++;
            Page page = new Page(spaceId, pageNo, pageType);

            log.debug("Allocated new page: pageNo={}, type={}", pageNo, pageType);
            return page;

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 读取指定页面
     *
     * @param pageNo 页号
     * @return Page 对象
     * @throws IOException IO 异常
     */
    public Page readPage(int pageNo) throws IOException {
        lock.readLock().lock();
        try {
            // 计算文件偏移量
            long offset = (long) pageNo * Page.PAGE_SIZE;

            // 检查页号是否有效
            if (offset >= raf.length()) {
                throw new IllegalArgumentException(
                        "Invalid pageNo: " + pageNo + ", file size: " + raf.length());
            }

            // 读取页面数据
            ByteBuffer buffer = ByteBuffer.allocate(Page.PAGE_SIZE);
            channel.position(offset);
            int bytesRead = channel.read(buffer);

            if (bytesRead != Page.PAGE_SIZE) {
                throw new IOException(
                        "Incomplete page read: expected " + Page.PAGE_SIZE + ", got " + bytesRead);
            }

            // 反序列化
            Page page = Page.deserialize(buffer.array());

            log.debug("Read page: pageNo={}, type={}", pageNo, page.getPageType());
            return page;

        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 写入指定页面
     *
     * @param page 要写入的页面
     * @throws IOException IO 异常
     */
    public void writePage(Page page) throws IOException {
        lock.writeLock().lock();
        try {
            int pageNo = page.getPageNo();
            long offset = (long) pageNo * Page.PAGE_SIZE;

            // 序列化页面
            byte[] data = page.serialize();

            // 写入文件
            ByteBuffer buffer = ByteBuffer.wrap(data);
            channel.position(offset);
            int bytesWritten = channel.write(buffer);

            if (bytesWritten != Page.PAGE_SIZE) {
                throw new IOException(
                        "Incomplete page write: expected " + Page.PAGE_SIZE + ", got " + bytesWritten);
            }

            // 标记为干净页
            page.markClean();

            log.debug("Wrote page: pageNo={}, type={}", pageNo, page.getPageType());

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 强制刷盘 (sync)
     *
     * @throws IOException IO 异常
     */
    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            channel.force(true); // 强制写入磁盘 (包括元数据)
            log.debug("Flushed tablespace: spaceId={}", spaceId);

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 关闭表空间
     *
     * @throws IOException IO 异常
     */
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            flush();
            channel.close();
            raf.close();

            log.info("Closed tablespace: spaceId={}, file={}", spaceId, file.getPath());

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取文件大小
     */
    public long getFileSize() throws IOException {
        return raf.length();
    }

    /**
     * 获取页数
     */
    public int getPageCount() {
        return nextPageNo;
    }

    /**
     * 获取表空间 ID
     */
    public int getSpaceId() {
        return spaceId;
    }

    /**
     * 获取文件路径
     */
    public String getFilePath() {
        return file.getAbsolutePath();
    }

    @Override
    public String toString() {
        try {
            return String.format("TableSpace{spaceId=%d, file=%s, size=%d bytes, pages=%d}",
                    spaceId, file.getName(), getFileSize(), nextPageNo);
        } catch (IOException e) {
            return String.format("TableSpace{spaceId=%d, file=%s}", spaceId, file.getName());
        }
    }
}
