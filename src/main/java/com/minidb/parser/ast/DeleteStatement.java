package com.minidb.parser.ast;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * DELETE 语句的 AST 节点
 *
 * 语法: DELETE FROM table [WHERE condition]
 *
 * @author Mini-MySQL
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class DeleteStatement extends SqlStatement {

    /**
     * 表名
     */
    private String tableName;

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
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        sb.append(tableName);

        if (whereCondition != null) {
            sb.append(" WHERE ").append(whereCondition);
        }

        return sb.toString();
    }
}
