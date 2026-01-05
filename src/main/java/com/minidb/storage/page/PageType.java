package com.minidb.storage.page;

/**
 * 页类型枚举
 *
 * MySQL InnoDB 有多种页类型:
 * - FIL_PAGE_TYPE_ALLOCATED: 最新分配,还未使用
 * - FIL_PAGE_UNDO_LOG: Undo Log 页
 * - FIL_PAGE_INODE: 段信息节点
 * - FIL_PAGE_IBUF_FREE_LIST: Insert Buffer 空闲列表
 * - FIL_PAGE_IBUF_BITMAP: Insert Buffer 位图
 * - FIL_PAGE_TYPE_SYS: 系统页
 * - FIL_PAGE_TYPE_TRX_SYS: 事务系统数据
 * - FIL_PAGE_TYPE_FSP_HDR: 表空间头部信息
 * - FIL_PAGE_INDEX: B+ 树索引页 (最常用)
 * - FIL_PAGE_TYPE_BLOB: BLOB 页
 *
 * 简化实现: 只实现最核心的几种
 *
 * @author Mini-MySQL
 */
public enum PageType {
    /**
     * 空闲页
     */
    FREE(0x0000),

    /**
     * B+ 树索引页 (数据页)
     * 包含用户记录
     */
    INDEX(0x45BF),

    /**
     * Undo Log 页
     */
    UNDO_LOG(0x0002),

    /**
     * 系统页
     */
    SYSTEM(0x0003),

    /**
     * 表空间头部页
     */
    FSP_HEADER(0x0008);

    private final int code;

    PageType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    /**
     * 根据代码获取页类型
     */
    public static PageType fromCode(int code) {
        for (PageType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        return FREE;
    }
}
