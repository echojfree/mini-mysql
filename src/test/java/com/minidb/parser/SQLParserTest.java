package com.minidb.parser;

import com.minidb.parser.ast.*;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQL 解析器的单元测试
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SQLParserTest {

    private SQLParser parser;

    @BeforeEach
    void setUp() {
        parser = new SQLParser();
    }

    /**
     * 测试解析 SELECT * 语句
     */
    @Test
    @Order(1)
    void testParseSelectAll() throws SQLParser.SQLParseException {
        String sql = "SELECT * FROM users";

        SqlStatement stmt = parser.parse(sql);

        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertTrue(select.isSelectAll());
        assertEquals("users", select.getTableName());
        assertNull(select.getWhereCondition());
    }

    /**
     * 测试解析 SELECT 指定列
     */
    @Test
    @Order(2)
    void testParseSelectColumns() throws SQLParser.SQLParseException {
        String sql = "SELECT id, name, age FROM users";

        SqlStatement stmt = parser.parse(sql);

        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertFalse(select.isSelectAll());
        assertEquals(3, select.getSelectElements().size());
        assertEquals("id", select.getSelectElements().get(0).getColumnName());
        assertEquals("name", select.getSelectElements().get(1).getColumnName());
        assertEquals("age", select.getSelectElements().get(2).getColumnName());
    }

    /**
     * 测试解析 SELECT 带别名
     */
    @Test
    @Order(3)
    void testParseSelectWithAlias() throws SQLParser.SQLParseException {
        String sql = "SELECT id, name AS userName, age FROM users";

        SqlStatement stmt = parser.parse(sql);

        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertEquals("id", select.getSelectElements().get(0).getColumnName());
        assertEquals("name", select.getSelectElements().get(1).getColumnName());
        assertEquals("userName", select.getSelectElements().get(1).getAlias());
    }

    /**
     * 测试解析 SELECT 带 WHERE 条件
     */
    @Test
    @Order(4)
    void testParseSelectWithWhere() throws SQLParser.SQLParseException {
        String sql = "SELECT * FROM users WHERE age > 18";

        SqlStatement stmt = parser.parse(sql);

        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertNotNull(select.getWhereCondition());

        Expression whereExpr = select.getWhereCondition();
        assertTrue(whereExpr instanceof Expression.BinaryExpression);
        Expression.BinaryExpression binExpr = (Expression.BinaryExpression) whereExpr;
        assertEquals(">", binExpr.getOperator());
    }

    /**
     * 测试解析 SELECT 带 ORDER BY
     */
    @Test
    @Order(5)
    void testParseSelectWithOrderBy() throws SQLParser.SQLParseException {
        String sql = "SELECT * FROM users ORDER BY age DESC, name ASC";

        SqlStatement stmt = parser.parse(sql);

        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertNotNull(select.getOrderByElements());
        assertEquals(2, select.getOrderByElements().size());
        assertEquals("age", select.getOrderByElements().get(0).getColumnName());
        assertTrue(select.getOrderByElements().get(0).isDescending());
        assertEquals("name", select.getOrderByElements().get(1).getColumnName());
        assertFalse(select.getOrderByElements().get(1).isDescending());
    }

    /**
     * 测试解析 SELECT 带 LIMIT
     */
    @Test
    @Order(6)
    void testParseSelectWithLimit() throws SQLParser.SQLParseException {
        String sql = "SELECT * FROM users LIMIT 10";

        SqlStatement stmt = parser.parse(sql);

        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertNotNull(select.getLimit());
        assertEquals(10, select.getLimit());
    }

    /**
     * 测试解析 SELECT 组合语句
     */
    @Test
    @Order(7)
    void testParseSelectComplex() throws SQLParser.SQLParseException {
        String sql = "SELECT id, name FROM users WHERE age > 18 ORDER BY name ASC LIMIT 20";

        SqlStatement stmt = parser.parse(sql);

        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertFalse(select.isSelectAll());
        assertEquals(2, select.getSelectElements().size());
        assertNotNull(select.getWhereCondition());
        assertNotNull(select.getOrderByElements());
        assertEquals(20, select.getLimit());
    }

    /**
     * 测试解析 INSERT 语句
     */
    @Test
    @Order(8)
    void testParseInsert() throws SQLParser.SQLParseException {
        String sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)";

        SqlStatement stmt = parser.parse(sql);

        assertTrue(stmt instanceof InsertStatement);
        InsertStatement insert = (InsertStatement) stmt;
        assertEquals("users", insert.getTableName());
        assertEquals(3, insert.getColumnNames().size());
        assertEquals("id", insert.getColumnNames().get(0));
        assertEquals("name", insert.getColumnNames().get(1));
        assertEquals("age", insert.getColumnNames().get(2));
        assertEquals(3, insert.getValues().size());
        assertEquals(1L, insert.getValues().get(0));
        assertEquals("Alice", insert.getValues().get(1));
        assertEquals(25L, insert.getValues().get(2));
    }

    /**
     * 测试解析 UPDATE 语句
     */
    @Test
    @Order(9)
    void testParseUpdate() throws SQLParser.SQLParseException {
        String sql = "UPDATE users SET name = 'Bob', age = 30 WHERE id = 1";

        SqlStatement stmt = parser.parse(sql);

        assertTrue(stmt instanceof UpdateStatement);
        UpdateStatement update = (UpdateStatement) stmt;
        assertEquals("users", update.getTableName());
        assertEquals(2, update.getAssignments().size());
        assertEquals("name", update.getAssignments().get(0).getColumnName());
        assertEquals("age", update.getAssignments().get(1).getColumnName());
        assertNotNull(update.getWhereCondition());
    }

    /**
     * 测试解析 DELETE 语句
     */
    @Test
    @Order(10)
    void testParseDelete() throws SQLParser.SQLParseException {
        String sql = "DELETE FROM users WHERE id = 1";

        SqlStatement stmt = parser.parse(sql);

        assertTrue(stmt instanceof DeleteStatement);
        DeleteStatement delete = (DeleteStatement) stmt;
        assertEquals("users", delete.getTableName());
        assertNotNull(delete.getWhereCondition());
    }

    /**
     * 测试解析 CREATE TABLE 语句
     */
    @Test
    @Order(11)
    void testParseCreateTable() throws SQLParser.SQLParseException {
        String sql = "CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100) NOT NULL, age INT)";

        SqlStatement stmt = parser.parse(sql);

        assertTrue(stmt instanceof CreateTableStatement);
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertEquals("users", create.getTableName());
        assertEquals(3, create.getColumnDefinitions().size());

        // 检查第一列
        CreateTableStatement.ColumnDefinition idCol = create.getColumnDefinitions().get(0);
        assertEquals("id", idCol.getColumnName());
        assertEquals("INT", idCol.getDataType());
        assertTrue(idCol.getConstraints().contains("PRIMARY KEY"));
        assertTrue(idCol.getConstraints().contains("AUTO_INCREMENT"));

        // 检查第二列
        CreateTableStatement.ColumnDefinition nameCol = create.getColumnDefinitions().get(1);
        assertEquals("name", nameCol.getColumnName());
        assertEquals("VARCHAR", nameCol.getDataType());
        assertEquals(100, nameCol.getTypeParameters().get(0));
        assertTrue(nameCol.getConstraints().contains("NOT NULL"));
    }

    /**
     * 测试解析 DROP TABLE 语句
     */
    @Test
    @Order(12)
    void testParseDropTable() throws SQLParser.SQLParseException {
        String sql = "DROP TABLE users";

        SqlStatement stmt = parser.parse(sql);

        assertTrue(stmt instanceof DropTableStatement);
        DropTableStatement drop = (DropTableStatement) stmt;
        assertEquals("users", drop.getTableName());
    }

    /**
     * 测试解析 WHERE 条件 - AND/OR
     */
    @Test
    @Order(13)
    void testParseWhereAndOr() throws SQLParser.SQLParseException {
        String sql = "SELECT * FROM users WHERE age > 18 AND name = 'Alice'";

        SqlStatement stmt = parser.parse(sql);

        SelectStatement select = (SelectStatement) stmt;
        assertTrue(select.getWhereCondition() instanceof Expression.BinaryExpression);
        Expression.BinaryExpression and = (Expression.BinaryExpression) select.getWhereCondition();
        assertEquals("AND", and.getOperator());
    }

    /**
     * 测试解析 WHERE 条件 - IN
     */
    @Test
    @Order(14)
    void testParseWhereIn() throws SQLParser.SQLParseException {
        String sql = "SELECT * FROM users WHERE id IN (1, 2, 3)";

        SqlStatement stmt = parser.parse(sql);

        SelectStatement select = (SelectStatement) stmt;
        assertTrue(select.getWhereCondition() instanceof Expression.InExpression);
        Expression.InExpression inExpr = (Expression.InExpression) select.getWhereCondition();
        assertEquals("id", inExpr.getColumnName());
        assertEquals(3, inExpr.getValues().size());
    }

    /**
     * 测试解析 WHERE 条件 - BETWEEN
     */
    @Test
    @Order(15)
    void testParseWhereBetween() throws SQLParser.SQLParseException {
        String sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 30";

        SqlStatement stmt = parser.parse(sql);

        SelectStatement select = (SelectStatement) stmt;
        assertTrue(select.getWhereCondition() instanceof Expression.BetweenExpression);
        Expression.BetweenExpression between = (Expression.BetweenExpression) select.getWhereCondition();
        assertEquals("age", between.getColumnName());
        assertEquals(18L, between.getStart());
        assertEquals(30L, between.getEnd());
    }

    /**
     * 测试解析 WHERE 条件 - IS NULL
     */
    @Test
    @Order(16)
    void testParseWhereIsNull() throws SQLParser.SQLParseException {
        String sql = "SELECT * FROM users WHERE name IS NULL";

        SqlStatement stmt = parser.parse(sql);

        SelectStatement select = (SelectStatement) stmt;
        assertTrue(select.getWhereCondition() instanceof Expression.UnaryExpression);
        Expression.UnaryExpression unary = (Expression.UnaryExpression) select.getWhereCondition();
        assertEquals("IS NULL", unary.getOperator());
    }

    /**
     * 测试大小写不敏感
     */
    @Test
    @Order(17)
    void testCaseInsensitive() throws SQLParser.SQLParseException {
        String sql = "select * from users where age > 18";

        SqlStatement stmt = parser.parse(sql);

        assertTrue(stmt instanceof SelectStatement);
        SelectStatement select = (SelectStatement) stmt;
        assertTrue(select.isSelectAll());
    }

    /**
     * 测试错误的 SQL 语句
     */
    @Test
    @Order(18)
    void testInvalidSQL() {
        String sql = "INVALID SQL STATEMENT";

        assertThrows(SQLParser.SQLParseException.class, () -> {
            parser.parse(sql);
        });
    }
}
