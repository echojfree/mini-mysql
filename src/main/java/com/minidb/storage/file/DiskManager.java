package com.minidb.storage.file;

import com.minidb.storage.page.Page;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 磁盘管理器（DiskManager）
 *
 * 磁盘管理器是文件 I/O 层的核心组件，负责管理所有表空间
 *
 * 核心功能：
 * 1. 创建和管理表空间
 * 2. 统一的页面读写接口
 * 3. 表空间的生命周期管理
 * 4. 全局的空间 ID 分配
 *
 * 设计模式：
 * - 单例模式：全局唯一的磁盘管理器
 * - 工厂模式：创建和管理表空间对象
 *
 * 对应八股文知识点：
 * ✅ InnoDB 如何管理多个表的数据文件？
 * ✅ 表空间 ID 的作用
 * ✅ 文件 I/O 的抽象层设计
 *
 * @author Mini-MySQL
 */
@Slf4j
public class DiskManager {

    /**
     * 数据目录
     */
    private final String dataDirectory;

    /**
     * 表空间映射表
     * Key: 表空间 ID
     * Value: TableSpace 对象
     */
    private final ConcurrentHashMap<Integer, TableSpace> tableSpaces;

    /**
     * 表空间名称到 ID 的映射
     * Key: 表空间名称
     * Value: 表空间 ID
     */
    private final ConcurrentHashMap<String, Integer> nameToIdMap;

    /**
     * 表空间 ID 生成器
     * 使用原子整数保证线程安全
     */
    private final AtomicInteger spaceIdGenerator;

    /**
     * 构造函数
     *
     * @param dataDirectory 数据目录路径
     */
    public DiskManager(String dataDirectory) {
        this.dataDirectory = dataDirectory;
        this.tableSpaces = new ConcurrentHashMap<>();
        this.nameToIdMap = new ConcurrentHashMap<>();
        this.spaceIdGenerator = new AtomicInteger(0);

        // 确保数据目录存在
        File dataDir = new File(dataDirectory);
        if (!dataDir.exists()) {
            if (!dataDir.mkdirs()) {
                log.error("Failed to create data directory: {}", dataDirectory);
            }
        }

        log.info("DiskManager initialized with data directory: {}", dataDirectory);
    }

    /**
     * 创建新的表空间
     *
     * @param spaceName 表空间名称
     * @return 表空间 ID
     * @throws IOException 文件操作异常
     */
    public int createTableSpace(String spaceName) throws IOException {
        // 检查表空间是否已存在
        if (nameToIdMap.containsKey(spaceName)) {
            throw new IllegalArgumentException("TableSpace already exists: " + spaceName);
        }

        // 分配新的表空间 ID
        int spaceId = spaceIdGenerator.getAndIncrement();

        // 创建表空间对象
        TableSpace tableSpace = new TableSpace(spaceId, spaceName, dataDirectory);

        // 打开表空间
        tableSpace.open();

        // 注册到映射表
        tableSpaces.put(spaceId, tableSpace);
        nameToIdMap.put(spaceName, spaceId);

        log.info("Created TableSpace: name={}, spaceId={}", spaceName, spaceId);

        return spaceId;
    }

    /**
     * 打开已存在的表空间
     *
     * @param spaceName 表空间名称
     * @return 表空间 ID
     * @throws IOException 文件操作异常
     */
    public int openTableSpace(String spaceName) throws IOException {
        // 如果已经打开，直接返回
        if (nameToIdMap.containsKey(spaceName)) {
            return nameToIdMap.get(spaceName);
        }

        // 检查文件是否存在
        String filePath = dataDirectory + File.separator + spaceName + ".ibd";
        File file = new File(filePath);
        if (!file.exists()) {
            throw new IOException("TableSpace file not found: " + filePath);
        }

        // 分配表空间 ID
        int spaceId = spaceIdGenerator.getAndIncrement();

        // 创建并打开表空间
        TableSpace tableSpace = new TableSpace(spaceId, spaceName, dataDirectory);
        tableSpace.open();

        // 注册到映射表
        tableSpaces.put(spaceId, tableSpace);
        nameToIdMap.put(spaceName, spaceId);

        log.info("Opened TableSpace: name={}, spaceId={}, pages={}",
                spaceName, spaceId, tableSpace.getPageCount());

        return spaceId;
    }

    /**
     * 获取或创建表空间
     * 如果表空间不存在则创建，否则打开
     *
     * @param spaceName 表空间名称
     * @return 表空间 ID
     * @throws IOException 文件操作异常
     */
    public int getOrCreateTableSpace(String spaceName) throws IOException {
        // 如果已经打开，直接返回
        if (nameToIdMap.containsKey(spaceName)) {
            return nameToIdMap.get(spaceName);
        }

        // 检查文件是否存在
        String filePath = dataDirectory + File.separator + spaceName + ".ibd";
        File file = new File(filePath);

        if (file.exists()) {
            return openTableSpace(spaceName);
        } else {
            return createTableSpace(spaceName);
        }
    }

    /**
     * 关闭表空间
     *
     * @param spaceId 表空间 ID
     * @throws IOException 文件操作异常
     */
    public void closeTableSpace(int spaceId) throws IOException {
        TableSpace tableSpace = tableSpaces.get(spaceId);
        if (tableSpace == null) {
            log.warn("TableSpace not found: spaceId={}", spaceId);
            return;
        }

        // 关闭表空间
        tableSpace.close();

        // 从映射表中移除
        tableSpaces.remove(spaceId);
        nameToIdMap.remove(tableSpace.getSpaceName());

        log.info("Closed TableSpace: spaceId={}, name={}", spaceId, tableSpace.getSpaceName());
    }

    /**
     * 删除表空间
     * 警告：此操作会删除物理文件，不可逆！
     *
     * @param spaceId 表空间 ID
     * @return 是否删除成功
     */
    public boolean deleteTableSpace(int spaceId) {
        TableSpace tableSpace = tableSpaces.get(spaceId);
        if (tableSpace == null) {
            log.warn("TableSpace not found: spaceId={}", spaceId);
            return false;
        }

        String spaceName = tableSpace.getSpaceName();

        // 删除表空间
        boolean deleted = tableSpace.delete();

        if (deleted) {
            // 从映射表中移除
            tableSpaces.remove(spaceId);
            nameToIdMap.remove(spaceName);
            log.info("Deleted TableSpace: spaceId={}, name={}", spaceId, spaceName);
        }

        return deleted;
    }

    /**
     * 分配新页面
     *
     * @param spaceId 表空间 ID
     * @return 新页面的页号
     * @throws IOException 文件操作异常
     */
    public int allocatePage(int spaceId) throws IOException {
        TableSpace tableSpace = getTableSpace(spaceId);
        return tableSpace.allocatePage();
    }

    /**
     * 写入页面
     *
     * @param spaceId 表空间 ID
     * @param page    要写入的页面
     * @throws IOException 文件操作异常
     */
    public void writePage(int spaceId, Page page) throws IOException {
        TableSpace tableSpace = getTableSpace(spaceId);
        tableSpace.writePage(page);
    }

    /**
     * 读取页面
     *
     * @param spaceId    表空间 ID
     * @param pageNumber 页号
     * @return 读取的页面
     * @throws IOException 文件操作异常
     */
    public Page readPage(int spaceId, int pageNumber) throws IOException {
        TableSpace tableSpace = getTableSpace(spaceId);
        return tableSpace.readPage(pageNumber);
    }

    /**
     * 同步所有表空间到磁盘
     *
     * @throws IOException 文件操作异常
     */
    public void syncAll() throws IOException {
        for (TableSpace tableSpace : tableSpaces.values()) {
            tableSpace.sync();
        }
        log.info("Synced all TableSpaces to disk");
    }

    /**
     * 关闭所有表空间
     *
     * @throws IOException 文件操作异常
     */
    public void closeAll() throws IOException {
        for (TableSpace tableSpace : tableSpaces.values()) {
            try {
                tableSpace.close();
            } catch (IOException e) {
                log.error("Failed to close TableSpace: {}", tableSpace.getSpaceName(), e);
            }
        }
        tableSpaces.clear();
        nameToIdMap.clear();
        log.info("Closed all TableSpaces");
    }

    /**
     * 获取表空间
     *
     * @param spaceId 表空间 ID
     * @return TableSpace 对象
     */
    public TableSpace getTableSpace(int spaceId) {
        TableSpace tableSpace = tableSpaces.get(spaceId);
        if (tableSpace == null) {
            throw new IllegalArgumentException("TableSpace not found: spaceId=" + spaceId);
        }
        return tableSpace;
    }

    /**
     * 根据名称获取表空间 ID
     *
     * @param spaceName 表空间名称
     * @return 表空间 ID，如果不存在返回 -1
     */
    public int getTableSpaceId(String spaceName) {
        return nameToIdMap.getOrDefault(spaceName, -1);
    }

    /**
     * 获取数据目录
     *
     * @return 数据目录路径
     */
    public String getDataDirectory() {
        return dataDirectory;
    }

    /**
     * 获取所有表空间的数量
     *
     * @return 表空间数量
     */
    public int getTableSpaceCount() {
        return tableSpaces.size();
    }
}
