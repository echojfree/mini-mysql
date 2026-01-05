package com.minidb.parser.ast;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * INSERT 语句的 AST 节点
 *
 * 语法: INSERT INTO table (col1, col2, ...) VALUES (val1, val2, ...)
 *
 * @author Mini-MySQL
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class InsertStatement extends SqlStatement {

    /**
     * 表名
     */
    private String tableName;

    /**
     * 列名列表
     */
    private List<String> columnNames;

    /**
     * 值列表
     */
    private List<Object> values;

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(tableName).append(" (");

        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(columnNames.get(i));
        }

        sb.append(") VALUES (");

        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(", ");
            Object value = values.get(i);
            if (value instanceof String) {
                sb.append("'").append(value).append("'");
            } else {
                sb.append(value);
            }
        }

        sb.append(")");
        return sb.toString();
    }
}
