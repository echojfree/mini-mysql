package com.minidb.parser;

import com.minidb.parser.ast.SqlStatement;
import lombok.extern.slf4j.Slf4j;
import org.antlr.v4.runtime.*;

/**
 * SQL 解析器入口
 *
 * 负责将 SQL 字符串解析为 AST (抽象语法树)
 *
 * 解析流程:
 * 1. 词法分析: SQL 字符串 → Token 流
 * 2. 语法分析: Token 流 → 解析树 (Parse Tree)
 * 3. AST 构建: 解析树 → AST (抽象语法树)
 *
 * 对应八股文知识点:
 * ✅ SQL 的执行流程
 * ✅ 词法分析 vs 语法分析
 * ✅ 什么是 AST?
 * ✅ 解析树 vs 抽象语法树
 *
 * @author Mini-MySQL
 */
@Slf4j
public class SQLParser {

    /**
     * 解析 SQL 语句
     *
     * @param sql SQL 字符串
     * @return AST 根节点
     * @throws SQLParseException 解析失败时抛出
     */
    public SqlStatement parse(String sql) throws SQLParseException {
        try {
            log.info("Parsing SQL: {}", sql);

            // 1. 词法分析: 创建词法分析器
            CharStream input = CharStreams.fromString(sql);
            MiniSQLLexer lexer = new MiniSQLLexer(input);

            // 添加错误监听器
            lexer.removeErrorListeners();
            lexer.addErrorListener(new ParseErrorListener());

            // 2. 生成 Token 流
            CommonTokenStream tokens = new CommonTokenStream(lexer);

            // 3. 语法分析: 创建语法分析器
            MiniSQLParser parser = new MiniSQLParser(tokens);
            parser.removeErrorListeners();
            parser.addErrorListener(new ParseErrorListener());

            // 4. 解析生成解析树
            MiniSQLParser.SqlStatementContext parseTree = parser.sqlStatement();

            // 5. 构建 AST
            ASTBuilder astBuilder = new ASTBuilder();
            SqlStatement ast = astBuilder.visitSqlStatement(parseTree);

            log.info("Parse successful: {}", ast.getStatementType());
            return ast;

        } catch (Exception e) {
            log.error("Parse failed: {}", e.getMessage());
            throw new SQLParseException("Failed to parse SQL: " + sql, e);
        }
    }

    /**
     * 错误监听器
     */
    private static class ParseErrorListener extends BaseErrorListener {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer,
                                Object offendingSymbol,
                                int line,
                                int charPositionInLine,
                                String msg,
                                RecognitionException e) {
            String error = String.format("Syntax error at line %d:%d - %s",
                    line, charPositionInLine, msg);
            throw new RuntimeException(error);
        }
    }

    /**
     * SQL 解析异常
     */
    public static class SQLParseException extends Exception {
        public SQLParseException(String message) {
            super(message);
        }

        public SQLParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
