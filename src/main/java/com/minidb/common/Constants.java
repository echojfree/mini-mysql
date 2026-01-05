package com.minidb.common;

/**
 * 系统常量定义
 *
 * @author Mini-MySQL
 */
public class Constants {

    // ==================== 页面相关常量 ====================

    /**
     * 页面大小：16KB
     * 这是 InnoDB 默认的页面大小，也是磁盘 I/O 的基本单位
     *
     * 为什么是 16KB？
     * 1. 兼顾内存利用率和磁盘 I/O 效率
     * 2. 太小会导致 B+ 树层数过高，增加查询次数
     * 3. 太大会浪费内存，降低缓冲池容量
     */
    public static final int PAGE_SIZE = 16 * 1024;

    /**
     * 页头大小：38 字节
     * 包含页面元数据信息
     */
    public static final int PAGE_HEADER_SIZE = 38;

    /**
     * 页尾大小：8 字节
     * 用于校验页面完整性
     */
    public static final int PAGE_TRAILER_SIZE = 8;

    /**
     * 页面数据区大小
     */
    public static final int PAGE_DATA_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE - PAGE_TRAILER_SIZE;

    // ==================== 页面类型 ====================

    /**
     * 数据页：存储用户数据行
     */
    public static final byte PAGE_TYPE_DATA = 0x01;

    /**
     * 索引页：存储 B+ 树非叶子节点
     */
    public static final byte PAGE_TYPE_INDEX = 0x02;

    /**
     * Undo 页：存储事务回滚信息
     */
    public static final byte PAGE_TYPE_UNDO = 0x03;

    /**
     * 系统页：存储系统元数据
     */
    public static final byte PAGE_TYPE_SYSTEM = 0x04;

    // ==================== 文件相关常量 ====================

    /**
     * 区（Extent）大小：1MB = 64 个页
     * InnoDB 以区为单位分配空间，提高顺序读写性能
     */
    public static final int EXTENT_SIZE = 64 * PAGE_SIZE;

    /**
     * 每个区包含的页数
     */
    public static final int PAGES_PER_EXTENT = 64;

    /**
     * 文件扩展名
     */
    public static final String DATA_FILE_SUFFIX = ".ibd";

    // ==================== B+ 树相关常量 ====================

    /**
     * B+ 树节点最小度数
     * 对于 16KB 页面，假设键值 + 指针总共 100 字节
     * 则一个页面大约可存储 160 个键值对
     */
    public static final int BTREE_ORDER = 160;

    /**
     * B+ 树叶子节点最小关键字数量
     */
    public static final int BTREE_MIN_KEYS = BTREE_ORDER / 2;

    /**
     * B+ 树非叶子节点最小子节点数量
     */
    public static final int BTREE_MIN_CHILDREN = BTREE_MIN_KEYS + 1;

    // ==================== 事务相关常量 ====================

    /**
     * 无效事务 ID
     */
    public static final long INVALID_TRX_ID = 0L;

    /**
     * 系统事务 ID（用于系统操作）
     */
    public static final long SYSTEM_TRX_ID = 1L;

    /**
     * 最小用户事务 ID
     */
    public static final long MIN_USER_TRX_ID = 100L;

    // ==================== 锁相关常量 ====================

    /**
     * 锁等待超时时间（毫秒）
     */
    public static final long LOCK_WAIT_TIMEOUT = 50000L;

    /**
     * 死锁检测间隔（毫秒）
     */
    public static final long DEADLOCK_DETECT_INTERVAL = 1000L;

    // ==================== 缓冲池相关常量 ====================

    /**
     * 缓冲池默认大小：128MB（8192 个页面）
     */
    public static final int DEFAULT_BUFFER_POOL_SIZE = 128 * 1024 * 1024;

    /**
     * LRU Young 区域占比（默认 5/8）
     */
    public static final double LRU_YOUNG_RATIO = 0.625;

    /**
     * LRU Old 区域占比（默认 3/8）
     */
    public static final double LRU_OLD_RATIO = 0.375;

    // ==================== 私有构造函数 ====================

    private Constants() {
        // 工具类，禁止实例化
    }
}
