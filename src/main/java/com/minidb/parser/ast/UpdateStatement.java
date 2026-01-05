package com.minidb.parser.ast;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * UPDATE 语句的 AST 节点
 *
 * 语法: UPDATE table SET col1=val1, col2=val2 [WHERE condition]
 *
 * @author Mini-MySQL
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class UpdateStatement extends SqlStatement {

    /**
     * 表名
     */
    private String tableName;

    /**
     * 赋值列表
     */
    private List<Assignment> assignments;

    /**
     * WHERE 条件表达式
     */
    private Expression whereCondition;

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(tableName).append(" SET ");

        for (int i = 0; i < assignments.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(assignments.get(i));
        }

        if (whereCondition != null) {
            sb.append(" WHERE ").append(whereCondition);
        }

        return sb.toString();
    }

    /**
     * 赋值 (column = value)
     */
    @Data
    public static class Assignment {
        /**
         * 列名
         */
        private String columnName;

        /**
         * 值表达式
         */
        private Expression value;

        @Override
        public String toString() {
            return columnName + " = " + value;
        }
    }
}
