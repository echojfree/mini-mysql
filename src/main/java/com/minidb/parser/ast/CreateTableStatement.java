package com.minidb.parser.ast;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * CREATE TABLE 语句的 AST 节点
 *
 * 语法: CREATE TABLE table_name (col1 type1 constraints, col2 type2, ...)
 *
 * @author Mini-MySQL
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class CreateTableStatement extends SqlStatement {

    /**
     * 表名
     */
    private String tableName;

    /**
     * 列定义列表
     */
    private List<ColumnDefinition> columnDefinitions;

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(tableName).append(" (");

        for (int i = 0; i < columnDefinitions.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(columnDefinitions.get(i));
        }

        sb.append(")");
        return sb.toString();
    }

    /**
     * 列定义
     */
    @Data
    public static class ColumnDefinition {
        /**
         * 列名
         */
        private String columnName;

        /**
         * 数据类型
         */
        private String dataType;

        /**
         * 类型参数 (例如 VARCHAR(100) 的 100)
         */
        private List<Integer> typeParameters;

        /**
         * 约束列表
         */
        private List<String> constraints;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(columnName).append(" ").append(dataType);

            if (typeParameters != null && !typeParameters.isEmpty()) {
                sb.append("(");
                for (int i = 0; i < typeParameters.size(); i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(typeParameters.get(i));
                }
                sb.append(")");
            }

            if (constraints != null) {
                for (String constraint : constraints) {
                    sb.append(" ").append(constraint);
                }
            }

            return sb.toString();
        }
    }
}
