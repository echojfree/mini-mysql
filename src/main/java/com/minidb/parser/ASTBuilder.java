package com.minidb.parser;

import com.minidb.parser.ast.*;
import lombok.extern.slf4j.Slf4j;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * AST 构建器
 *
 * 使用 Visitor 模式将 ANTLR 生成的解析树转换为我们自定义的 AST
 *
 * 对应八股文知识点:
 * ✅ 什么是 AST (抽象语法树)?
 * ✅ SQL 解析的流程
 * ✅ 访问者模式的应用
 *
 * @author Mini-MySQL
 */
@Slf4j
public class ASTBuilder extends MiniSQLBaseVisitor<Object> {

    /**
     * 访问 SQL 语句根节点
     */
    @Override
    public SqlStatement visitSqlStatement(MiniSQLParser.SqlStatementContext ctx) {
        log.debug("Visiting SQL statement");

        if (ctx.selectStatement() != null) {
            return visitSelectStatement(ctx.selectStatement());
        } else if (ctx.insertStatement() != null) {
            return visitInsertStatement(ctx.insertStatement());
        } else if (ctx.updateStatement() != null) {
            return visitUpdateStatement(ctx.updateStatement());
        } else if (ctx.deleteStatement() != null) {
            return visitDeleteStatement(ctx.deleteStatement());
        } else if (ctx.createTableStatement() != null) {
            return visitCreateTableStatement(ctx.createTableStatement());
        } else if (ctx.dropTableStatement() != null) {
            return visitDropTableStatement(ctx.dropTableStatement());
        }

        throw new IllegalArgumentException("Unknown statement type");
    }

    /**
     * 访问 SELECT 语句
     */
    @Override
    public SelectStatement visitSelectStatement(MiniSQLParser.SelectStatementContext ctx) {
        log.debug("Visiting SELECT statement");

        SelectStatement stmt = new SelectStatement();

        // 处理 SELECT 列
        if (ctx.selectElements().STAR() != null) {
            stmt.setSelectAll(true);
        } else {
            List<SelectStatement.SelectElement> elements = new ArrayList<>();
            for (MiniSQLParser.SelectElementContext elemCtx : ctx.selectElements().selectElement()) {
                SelectStatement.SelectElement element = new SelectStatement.SelectElement();
                element.setColumnName(elemCtx.columnName().getText());

                if (elemCtx.alias() != null) {
                    element.setAlias(elemCtx.alias().getText());
                }

                elements.add(element);
            }
            stmt.setSelectElements(elements);
        }

        // 表名
        stmt.setTableName(ctx.tableName().getText());

        // WHERE 子句
        if (ctx.whereClause() != null) {
            stmt.setWhereCondition(visitExpression(ctx.whereClause().expression()));
        }

        // ORDER BY 子句
        if (ctx.orderByClause() != null) {
            List<SelectStatement.OrderByElement> orderByElements = new ArrayList<>();
            for (MiniSQLParser.OrderByElementContext orderByCtx : ctx.orderByClause().orderByElement()) {
                SelectStatement.OrderByElement element = new SelectStatement.OrderByElement();
                element.setColumnName(orderByCtx.columnName().getText());
                element.setDescending(orderByCtx.DESC() != null);
                orderByElements.add(element);
            }
            stmt.setOrderByElements(orderByElements);
        }

        // LIMIT 子句
        if (ctx.limitClause() != null) {
            stmt.setLimit(Integer.parseInt(ctx.limitClause().INTEGER_LITERAL().getText()));
        }

        return stmt;
    }

    /**
     * 访问 INSERT 语句
     */
    @Override
    public InsertStatement visitInsertStatement(MiniSQLParser.InsertStatementContext ctx) {
        log.debug("Visiting INSERT statement");

        InsertStatement stmt = new InsertStatement();
        stmt.setTableName(ctx.tableName().getText());

        // 列名列表
        List<String> columnNames = ctx.columnName().stream()
                .map(colCtx -> colCtx.getText())
                .collect(Collectors.toList());
        stmt.setColumnNames(columnNames);

        // 值列表
        List<Object> values = ctx.constant().stream()
                .map(this::parseConstant)
                .collect(Collectors.toList());
        stmt.setValues(values);

        return stmt;
    }

    /**
     * 访问 UPDATE 语句
     */
    @Override
    public UpdateStatement visitUpdateStatement(MiniSQLParser.UpdateStatementContext ctx) {
        log.debug("Visiting UPDATE statement");

        UpdateStatement stmt = new UpdateStatement();
        stmt.setTableName(ctx.tableName().getText());

        // 赋值列表
        List<UpdateStatement.Assignment> assignments = new ArrayList<>();
        for (MiniSQLParser.AssignmentContext assignCtx : ctx.assignmentList().assignment()) {
            UpdateStatement.Assignment assignment = new UpdateStatement.Assignment();
            assignment.setColumnName(assignCtx.columnName().getText());
            assignment.setValue(visitExpression(assignCtx.expression()));
            assignments.add(assignment);
        }
        stmt.setAssignments(assignments);

        // WHERE 子句
        if (ctx.whereClause() != null) {
            stmt.setWhereCondition(visitExpression(ctx.whereClause().expression()));
        }

        return stmt;
    }

    /**
     * 访问 DELETE 语句
     */
    @Override
    public DeleteStatement visitDeleteStatement(MiniSQLParser.DeleteStatementContext ctx) {
        log.debug("Visiting DELETE statement");

        DeleteStatement stmt = new DeleteStatement();
        stmt.setTableName(ctx.tableName().getText());

        // WHERE 子句
        if (ctx.whereClause() != null) {
            stmt.setWhereCondition(visitExpression(ctx.whereClause().expression()));
        }

        return stmt;
    }

    /**
     * 访问 CREATE TABLE 语句
     */
    @Override
    public CreateTableStatement visitCreateTableStatement(MiniSQLParser.CreateTableStatementContext ctx) {
        log.debug("Visiting CREATE TABLE statement");

        CreateTableStatement stmt = new CreateTableStatement();
        stmt.setTableName(ctx.tableName().getText());

        // 列定义列表
        List<CreateTableStatement.ColumnDefinition> columnDefs = new ArrayList<>();
        for (MiniSQLParser.ColumnDefinitionContext colDefCtx : ctx.columnDefinition()) {
            CreateTableStatement.ColumnDefinition colDef = new CreateTableStatement.ColumnDefinition();
            colDef.setColumnName(colDefCtx.columnName().getText());
            colDef.setDataType(parseDataType(colDefCtx.dataType()));

            // 类型参数
            if (colDefCtx.dataType().INTEGER_LITERAL() != null && !colDefCtx.dataType().INTEGER_LITERAL().isEmpty()) {
                List<Integer> params = colDefCtx.dataType().INTEGER_LITERAL().stream()
                        .map(node -> Integer.parseInt(node.getText()))
                        .collect(Collectors.toList());
                colDef.setTypeParameters(params);
            }

            // 约束
            if (colDefCtx.columnConstraint() != null) {
                List<String> constraints = new ArrayList<>();
                for (MiniSQLParser.ColumnConstraintContext constraintCtx : colDefCtx.columnConstraint()) {
                    if (constraintCtx.PRIMARY() != null) {
                        constraints.add("PRIMARY KEY");
                    } else if (constraintCtx.NOT() != null) {
                        constraints.add("NOT NULL");
                    } else if (constraintCtx.AUTO_INCREMENT() != null) {
                        constraints.add("AUTO_INCREMENT");
                    } else if (constraintCtx.DEFAULT() != null) {
                        constraints.add("DEFAULT " + parseConstant(constraintCtx.constant()));
                    }
                }
                colDef.setConstraints(constraints);
            }

            columnDefs.add(colDef);
        }
        stmt.setColumnDefinitions(columnDefs);

        return stmt;
    }

    /**
     * 访问 DROP TABLE 语句
     */
    @Override
    public DropTableStatement visitDropTableStatement(MiniSQLParser.DropTableStatementContext ctx) {
        log.debug("Visiting DROP TABLE statement");

        DropTableStatement stmt = new DropTableStatement();
        stmt.setTableName(ctx.tableName().getText());
        return stmt;
    }

    /**
     * 访问表达式
     */
    @Override
    public Expression visitExpression(MiniSQLParser.ExpressionContext ctx) {
        // 处理 AND/OR 逻辑运算
        if (ctx.AND() != null || ctx.OR() != null) {
            Expression.BinaryExpression expr = new Expression.BinaryExpression();
            expr.setOperator(ctx.AND() != null ? "AND" : "OR");
            expr.setLeft(visitExpression(ctx.expression(0)));
            expr.setRight(visitExpression(ctx.expression(1)));
            return expr;
        }

        // 处理 NOT
        if (ctx.NOT() != null) {
            Expression.UnaryExpression expr = new Expression.UnaryExpression();
            expr.setOperator("NOT");
            expr.setOperand(visitExpression(ctx.expression(0)));
            return expr;
        }

        // 处理常量 (用于 UPDATE SET)
        if (ctx.constant() != null) {
            Expression.LiteralExpression expr = new Expression.LiteralExpression();
            Object value = parseConstant(ctx.constant());
            expr.setValue(value);
            expr.setDataType(getDataType(value));
            return expr;
        }

        // 处理谓词
        if (ctx.predicate() != null) {
            return visitPredicate(ctx.predicate());
        }

        throw new IllegalArgumentException("Unknown expression type");
    }

    /**
     * 访问谓词 (比较表达式)
     */
    @Override
    public Expression visitPredicate(MiniSQLParser.PredicateContext ctx) {
        // 处理括号表达式
        if (ctx.expression() != null) {
            return visitExpression(ctx.expression());
        }

        String columnName = ctx.columnName().getText();

        // 处理 IS NULL / IS NOT NULL
        if (ctx.IS() != null) {
            Expression.UnaryExpression expr = new Expression.UnaryExpression();
            Expression.ColumnReference colRef = new Expression.ColumnReference();
            colRef.setColumnName(columnName);
            expr.setOperand(colRef);
            expr.setOperator(ctx.NOT() != null ? "IS NOT NULL" : "IS NULL");
            return expr;
        }

        // 处理 IN
        if (ctx.IN() != null) {
            Expression.InExpression expr = new Expression.InExpression();
            expr.setColumnName(columnName);
            List<Object> values = ctx.constant().stream()
                    .map(this::parseConstant)
                    .collect(Collectors.toList());
            expr.setValues(values);
            return expr;
        }

        // 处理 BETWEEN
        if (ctx.BETWEEN() != null) {
            Expression.BetweenExpression expr = new Expression.BetweenExpression();
            expr.setColumnName(columnName);
            expr.setStart(parseConstant(ctx.constant(0)));
            expr.setEnd(parseConstant(ctx.constant(1)));
            return expr;
        }

        // 处理比较运算 (=, <, >, etc.)
        if (ctx.comparisonOperator() != null) {
            Expression.BinaryExpression expr = new Expression.BinaryExpression();
            Expression.ColumnReference colRef = new Expression.ColumnReference();
            colRef.setColumnName(columnName);
            expr.setLeft(colRef);
            expr.setOperator(ctx.comparisonOperator().getText());

            Expression.LiteralExpression literal = new Expression.LiteralExpression();
            Object value = parseConstant(ctx.constant(0));
            literal.setValue(value);
            literal.setDataType(getDataType(value));
            expr.setRight(literal);

            return expr;
        }

        throw new IllegalArgumentException("Unknown predicate type");
    }

    /**
     * 解析常量值
     */
    private Object parseConstant(MiniSQLParser.ConstantContext ctx) {
        if (ctx.INTEGER_LITERAL() != null) {
            return Long.parseLong(ctx.INTEGER_LITERAL().getText());
        } else if (ctx.STRING_LITERAL() != null) {
            String text = ctx.STRING_LITERAL().getText();
            // 移除引号
            return text.substring(1, text.length() - 1);
        } else if (ctx.NULL_LITERAL() != null) {
            return null;
        } else if (ctx.TRUE() != null) {
            return true;
        } else if (ctx.FALSE() != null) {
            return false;
        }
        throw new IllegalArgumentException("Unknown constant type: " + ctx.getText());
    }

    /**
     * 解析数据类型
     */
    private String parseDataType(MiniSQLParser.DataTypeContext ctx) {
        if (ctx.INT() != null) {
            return "INT";
        } else if (ctx.VARCHAR() != null) {
            return "VARCHAR";
        } else if (ctx.TEXT() != null) {
            return "TEXT";
        } else if (ctx.BIGINT() != null) {
            return "BIGINT";
        } else if (ctx.DECIMAL() != null) {
            return "DECIMAL";
        }
        throw new IllegalArgumentException("Unknown data type");
    }

    /**
     * 获取值的数据类型
     */
    private String getDataType(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof String) {
            return "STRING";
        } else if (value instanceof Number) {
            return "INTEGER";
        } else if (value instanceof Boolean) {
            return "BOOLEAN";
        }
        return "UNKNOWN";
    }
}
