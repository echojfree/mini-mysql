package com.minidb.parser.ast;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * SELECT 语句的 AST 节点
 *
 * 语法: SELECT columns FROM table [WHERE condition] [ORDER BY ...] [LIMIT n]
 *
 * @author Mini-MySQL
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class SelectStatement extends SqlStatement {

    /**
     * 选择的列(列表达式)
     */
    private List<SelectElement> selectElements;

    /**
     * 表名
     */
    private String tableName;

    /**
     * WHERE 条件表达式
     */
    private Expression whereCondition;

    /**
     * ORDER BY 子句
     */
    private List<OrderByElement> orderByElements;

    /**
     * LIMIT 数量
     */
    private Integer limit;

    /**
     * 是否是 SELECT *
     */
    private boolean selectAll;

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SELECT ");
        if (selectAll) {
            sb.append("* ");
        } else {
            for (int i = 0; i < selectElements.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(selectElements.get(i));
            }
        }
        sb.append(" FROM ").append(tableName);

        if (whereCondition != null) {
            sb.append(" WHERE ").append(whereCondition);
        }

        if (orderByElements != null && !orderByElements.isEmpty()) {
            sb.append(" ORDER BY ");
            for (int i = 0; i < orderByElements.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(orderByElements.get(i));
            }
        }

        if (limit != null) {
            sb.append(" LIMIT ").append(limit);
        }

        return sb.toString();
    }

    /**
     * SELECT 列元素
     */
    @Data
    public static class SelectElement {
        /**
         * 列名
         */
        private String columnName;

        /**
         * 别名
         */
        private String alias;

        @Override
        public String toString() {
            if (alias != null) {
                return columnName + " AS " + alias;
            }
            return columnName;
        }
    }

    /**
     * ORDER BY 元素
     */
    @Data
    public static class OrderByElement {
        /**
         * 列名
         */
        private String columnName;

        /**
         * 是否降序(true=DESC, false=ASC)
         */
        private boolean descending;

        @Override
        public String toString() {
            return columnName + (descending ? " DESC" : " ASC");
        }
    }
}
