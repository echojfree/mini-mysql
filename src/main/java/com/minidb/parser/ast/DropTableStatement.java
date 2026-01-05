package com.minidb.parser.ast;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * DROP TABLE 语句的 AST 节点
 *
 * 语法: DROP TABLE table_name
 *
 * @author Mini-MySQL
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class DropTableStatement extends SqlStatement {

    /**
     * 表名
     */
    private String tableName;

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return "DROP TABLE " + tableName;
    }
}
