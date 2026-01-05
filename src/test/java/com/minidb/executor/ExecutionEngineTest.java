package com.minidb.executor;

import com.minidb.parser.SQLParser;
import com.minidb.parser.ast.SelectStatement;
import com.minidb.parser.ast.SqlStatement;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 执行引擎测试
 *
 * 测试火山模型的各个算子和查询执行器
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ExecutionEngineTest {

    private QueryExecutor executor;
    private SQLParser parser;
    private Map<String, List<Map<String, Object>>> tableData;

    @BeforeEach
    void setUp() {
        executor = new QueryExecutor();
        parser = new SQLParser();

        // 准备测试数据
        tableData = new HashMap<>();
        List<Map<String, Object>> users = new ArrayList<>();

        users.add(createUser(1, "Alice", 25));
        users.add(createUser(2, "Bob", 30));
        users.add(createUser(3, "Charlie", 25));
        users.add(createUser(4, "David", 35));
        users.add(createUser(5, "Eve", 28));

        tableData.put("users", users);
    }

    /**
     * 测试简单的 SELECT *
     */
    @Test
    @Order(1)
    void testSelectAll() throws Exception {
        String sql = "SELECT * FROM users";
        SqlStatement stmt = parser.parse(sql);
        assertTrue(stmt instanceof SelectStatement);

        List<Map<String, Object>> results = executor.execute((SelectStatement) stmt, tableData);

        assertEquals(5, results.size());
        System.out.println("\n=== SELECT * FROM users ===");
        printResults(results);
    }

    /**
     * 测试 SELECT 指定列
     */
    @Test
    @Order(2)
    void testSelectColumns() throws Exception {
        String sql = "SELECT name, age FROM users";
        SqlStatement stmt = parser.parse(sql);

        List<Map<String, Object>> results = executor.execute((SelectStatement) stmt, tableData);

        assertEquals(5, results.size());
        for (Map<String, Object> row : results) {
            assertEquals(2, row.size());
            assertTrue(row.containsKey("name"));
            assertTrue(row.containsKey("age"));
            assertFalse(row.containsKey("id"));
        }

        System.out.println("\n=== SELECT name, age FROM users ===");
        printResults(results);
    }

    /**
     * 测试 WHERE 条件
     */
    @Test
    @Order(3)
    void testWhereCondition() throws Exception {
        String sql = "SELECT * FROM users WHERE age > 25";
        SqlStatement stmt = parser.parse(sql);

        List<Map<String, Object>> results = executor.execute((SelectStatement) stmt, tableData);

        assertEquals(3, results.size()); // Bob(30), David(35), Eve(28)
        for (Map<String, Object> row : results) {
            int age = (int) row.get("age");
            assertTrue(age > 25);
        }

        System.out.println("\n=== SELECT * FROM users WHERE age > 25 ===");
        printResults(results);
    }

    /**
     * 测试 ORDER BY
     */
    @Test
    @Order(4)
    void testOrderBy() throws Exception {
        String sql = "SELECT * FROM users ORDER BY age DESC";
        SqlStatement stmt = parser.parse(sql);

        List<Map<String, Object>> results = executor.execute((SelectStatement) stmt, tableData);

        assertEquals(5, results.size());

        // 验证排序: 应该是 35, 30, 28, 25, 25
        assertEquals(35, results.get(0).get("age"));
        assertEquals(30, results.get(1).get("age"));
        assertEquals(28, results.get(2).get("age"));

        System.out.println("\n=== SELECT * FROM users ORDER BY age DESC ===");
        printResults(results);
    }

    /**
     * 测试 LIMIT
     */
    @Test
    @Order(5)
    void testLimit() throws Exception {
        String sql = "SELECT * FROM users LIMIT 3";
        SqlStatement stmt = parser.parse(sql);

        List<Map<String, Object>> results = executor.execute((SelectStatement) stmt, tableData);

        assertEquals(3, results.size());

        System.out.println("\n=== SELECT * FROM users LIMIT 3 ===");
        printResults(results);
    }

    /**
     * 测试组合查询
     */
    @Test
    @Order(6)
    void testComplexQuery() throws Exception {
        String sql = "SELECT name, age FROM users WHERE age >= 25 ORDER BY age ASC LIMIT 3";
        SqlStatement stmt = parser.parse(sql);

        List<Map<String, Object>> results = executor.execute((SelectStatement) stmt, tableData);

        assertEquals(3, results.size());

        // 验证列
        for (Map<String, Object> row : results) {
            assertEquals(2, row.size());
            assertTrue(row.containsKey("name"));
            assertTrue(row.containsKey("age"));
        }

        // 验证过滤和排序
        assertEquals(25, results.get(0).get("age")); // Alice 或 Charlie
        assertEquals(25, results.get(1).get("age")); // Alice 或 Charlie
        assertEquals(28, results.get(2).get("age")); // Eve

        System.out.println("\n=== Complex Query ===");
        System.out.println("SQL: " + sql);
        printResults(results);
    }

    /**
     * 测试 ORDER BY 多列
     */
    @Test
    @Order(7)
    void testOrderByMultipleColumns() throws Exception {
        String sql = "SELECT * FROM users ORDER BY age ASC, name DESC";
        SqlStatement stmt = parser.parse(sql);

        List<Map<String, Object>> results = executor.execute((SelectStatement) stmt, tableData);

        assertEquals(5, results.size());

        // age=25 的两条记录，name 应该按降序排列
        // Charlie 应该在 Alice 之前
        assertEquals(25, results.get(0).get("age"));
        assertEquals(25, results.get(1).get("age"));

        System.out.println("\n=== ORDER BY age ASC, name DESC ===");
        printResults(results);
    }

    /**
     * 测试 WHERE + ORDER BY + LIMIT 组合
     */
    @Test
    @Order(8)
    void testFullPipeline() throws Exception {
        String sql = "SELECT id, name, age FROM users WHERE age > 25 ORDER BY age DESC LIMIT 2";
        SqlStatement stmt = parser.parse(sql);

        List<Map<String, Object>> results = executor.execute((SelectStatement) stmt, tableData);

        assertEquals(2, results.size());

        // 验证结果
        assertEquals(35, results.get(0).get("age")); // David
        assertEquals(30, results.get(1).get("age")); // Bob

        System.out.println("\n=== Full Pipeline Test ===");
        System.out.println("SQL: " + sql);
        printResults(results);
    }

    /**
     * 辅助方法: 创建用户数据
     */
    private Map<String, Object> createUser(int id, String name, int age) {
        Map<String, Object> user = new HashMap<>();
        user.put("id", id);
        user.put("name", name);
        user.put("age", age);
        return user;
    }

    /**
     * 辅助方法: 打印结果
     */
    private void printResults(List<Map<String, Object>> results) {
        if (results.isEmpty()) {
            System.out.println("(empty result)");
            return;
        }

        // 打印表头
        Set<String> columns = results.get(0).keySet();
        for (String col : columns) {
            System.out.printf("%-15s", col);
        }
        System.out.println();

        // 打印分隔线
        StringBuilder separator = new StringBuilder();
        for (int i = 0; i < columns.size() * 15; i++) {
            separator.append("-");
        }
        System.out.println(separator);

        // 打印数据
        for (Map<String, Object> row : results) {
            for (String col : columns) {
                System.out.printf("%-15s", row.get(col));
            }
            System.out.println();
        }
        System.out.println();
    }
}
