package com.minidb.parser.ast;

import lombok.Data;

/**
 * 表达式抽象基类
 *
 * 用于表示 WHERE 条件、赋值表达式等
 *
 * @author Mini-MySQL
 */
@Data
public abstract class Expression {

    /**
     * 表达式类型
     */
    public enum ExpressionType {
        /** 二元运算表达式 (AND, OR, =, <, >, etc.) */
        BINARY,
        /** 一元运算表达式 (NOT, IS NULL, etc.) */
        UNARY,
        /** 字面量 (常量值) */
        LITERAL,
        /** 列引用 */
        COLUMN_REF,
        /** IN 表达式 */
        IN,
        /** BETWEEN 表达式 */
        BETWEEN
    }

    /**
     * 获取表达式类型
     *
     * @return 表达式类型
     */
    public abstract ExpressionType getExpressionType();

    /**
     * 二元运算表达式
     */
    @Data
    public static class BinaryExpression extends Expression {
        /**
         * 运算符
         */
        private String operator;

        /**
         * 左操作数
         */
        private Expression left;

        /**
         * 右操作数
         */
        private Expression right;

        @Override
        public ExpressionType getExpressionType() {
            return ExpressionType.BINARY;
        }

        @Override
        public String toString() {
            return "(" + left + " " + operator + " " + right + ")";
        }
    }

    /**
     * 一元运算表达式
     */
    @Data
    public static class UnaryExpression extends Expression {
        /**
         * 运算符 (NOT, IS NULL, IS NOT NULL)
         */
        private String operator;

        /**
         * 操作数
         */
        private Expression operand;

        @Override
        public ExpressionType getExpressionType() {
            return ExpressionType.UNARY;
        }

        @Override
        public String toString() {
            if ("IS NULL".equals(operator) || "IS NOT NULL".equals(operator)) {
                return operand + " " + operator;
            }
            return operator + " " + operand;
        }
    }

    /**
     * 字面量表达式 (常量)
     */
    @Data
    public static class LiteralExpression extends Expression {
        /**
         * 字面量值
         */
        private Object value;

        /**
         * 数据类型 (INTEGER, STRING, NULL, BOOLEAN)
         */
        private String dataType;

        @Override
        public ExpressionType getExpressionType() {
            return ExpressionType.LITERAL;
        }

        @Override
        public String toString() {
            if ("STRING".equals(dataType)) {
                return "'" + value + "'";
            }
            return String.valueOf(value);
        }
    }

    /**
     * 列引用表达式
     */
    @Data
    public static class ColumnReference extends Expression {
        /**
         * 列名
         */
        private String columnName;

        @Override
        public ExpressionType getExpressionType() {
            return ExpressionType.COLUMN_REF;
        }

        @Override
        public String toString() {
            return columnName;
        }
    }

    /**
     * IN 表达式
     */
    @Data
    public static class InExpression extends Expression {
        /**
         * 列名
         */
        private String columnName;

        /**
         * 值列表
         */
        private java.util.List<Object> values;

        @Override
        public ExpressionType getExpressionType() {
            return ExpressionType.IN;
        }

        @Override
        public String toString() {
            return columnName + " IN (" + String.join(", ", values.stream()
                    .map(String::valueOf).toArray(String[]::new)) + ")";
        }
    }

    /**
     * BETWEEN 表达式
     */
    @Data
    public static class BetweenExpression extends Expression {
        /**
         * 列名
         */
        private String columnName;

        /**
         * 范围起始值
         */
        private Object start;

        /**
         * 范围结束值
         */
        private Object end;

        @Override
        public ExpressionType getExpressionType() {
            return ExpressionType.BETWEEN;
        }

        @Override
        public String toString() {
            return columnName + " BETWEEN " + start + " AND " + end;
        }
    }
}
