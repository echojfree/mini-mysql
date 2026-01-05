package com.minidb.executor;

import com.minidb.executor.operator.*;
import com.minidb.storage.index.BPlusTree;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 索引扫描和 JOIN 算子测试
 *
 * 测试 IndexScan 和 NestedLoopJoin 算子
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IndexAndJoinTest {

    private BPlusTree<Integer, Map<String, Object>> userIndex;
    private BPlusTree<Integer, Map<String, Object>> orderIndex;

    @BeforeEach
    void setUp() {
        // 创建用户表的 B+ 树索引（主键索引）
        userIndex = new BPlusTree<>(3);

        userIndex.insert(1, createUser(1, "Alice", 25));
        userIndex.insert(2, createUser(2, "Bob", 30));
        userIndex.insert(3, createUser(3, "Charlie", 25));
        userIndex.insert(4, createUser(4, "David", 35));
        userIndex.insert(5, createUser(5, "Eve", 28));

        // 创建订单表的 B+ 树索引（主键索引）
        orderIndex = new BPlusTree<>(3);

        orderIndex.insert(101, createOrder(101, 1, 100.0));  // Alice 的订单
        orderIndex.insert(102, createOrder(102, 1, 200.0));  // Alice 的订单
        orderIndex.insert(103, createOrder(103, 2, 150.0));  // Bob 的订单
        orderIndex.insert(104, createOrder(104, 3, 300.0));  // Charlie 的订单
        orderIndex.insert(105, createOrder(105, 5, 250.0));  // Eve 的订单
    }

    /**
     * 测试精确查询（等值查询）
     */
    @Test
    @Order(1)
    void testIndexScanExactMatch() throws Exception {
        IndexScanOperator scan = new IndexScanOperator("users", userIndex, 3);

        List<Map<String, Object>> results = executeOperator(scan);

        assertEquals(1, results.size());
        assertEquals("Charlie", results.get(0).get("name"));
        assertEquals(25, results.get(0).get("age"));

        System.out.println("\n=== IndexScan Exact Match (id=3) ===");
        printResults(results);
    }

    /**
     * 测试范围查询
     */
    @Test
    @Order(2)
    void testIndexScanRangeQuery() throws Exception {
        // 查询 id >= 2 AND id <= 4
        IndexScanOperator scan = new IndexScanOperator("users", userIndex, 2, 4);

        List<Map<String, Object>> results = executeOperator(scan);

        assertEquals(3, results.size()); // Bob, Charlie, David

        System.out.println("\n=== IndexScan Range Query (2 <= id <= 4) ===");
        printResults(results);
    }

    /**
     * 测试索引扫描 + Filter 组合
     */
    @Test
    @Order(3)
    void testIndexScanWithFilter() throws Exception {
        // 先用索引扫描获取范围数据
        IndexScanOperator scan = new IndexScanOperator("users", userIndex, 1, 5);

        // 再用 Filter 过滤 age > 25
        FilterOperator filter = new FilterOperator(
                scan,
                row -> (int) row.get("age") > 25,
                "age > 25"
        );

        List<Map<String, Object>> results = executeOperator(filter);

        assertEquals(3, results.size()); // Bob(30), David(35), Eve(28)

        System.out.println("\n=== IndexScan + Filter (age > 25) ===");
        printResults(results);
    }

    /**
     * 测试 INNER JOIN
     */
    @Test
    @Order(4)
    void testInnerJoin() throws Exception {
        // 左表: users
        TableScanOperator leftScan = new TableScanOperator("users",
                Arrays.asList(
                        createUser(1, "Alice", 25),
                        createUser(2, "Bob", 30),
                        createUser(3, "Charlie", 25)
                ));

        // 右表: orders
        TableScanOperator rightScan = new TableScanOperator("orders",
                Arrays.asList(
                        createOrder(101, 1, 100.0),
                        createOrder(102, 1, 200.0),
                        createOrder(103, 2, 150.0)
                ));

        // JOIN 条件: users.id = orders.user_id
        NestedLoopJoinOperator join = new NestedLoopJoinOperator(
                leftScan,
                rightScan,
                NestedLoopJoinOperator.JoinType.INNER,
                NestedLoopJoinOperator.equalsCondition("id", "user_id")
        );

        List<Map<String, Object>> results = executeOperator(join);

        // Alice 有 2 个订单, Bob 有 1 个订单
        assertEquals(3, results.size());

        // 验证第一条结果
        Map<String, Object> first = results.get(0);
        assertEquals("Alice", first.get("name"));
        assertEquals(101, first.get("order_id"));

        System.out.println("\n=== INNER JOIN (users x orders) ===");
        printResults(results);
    }

    /**
     * 测试 LEFT JOIN
     */
    @Test
    @Order(5)
    void testLeftJoin() throws Exception {
        // 左表: users (包含没有订单的用户)
        TableScanOperator leftScan = new TableScanOperator("users",
                Arrays.asList(
                        createUser(1, "Alice", 25),
                        createUser(2, "Bob", 30),
                        createUser(4, "David", 35)  // David 没有订单
                ));

        // 右表: orders
        TableScanOperator rightScan = new TableScanOperator("orders",
                Arrays.asList(
                        createOrder(101, 1, 100.0),
                        createOrder(103, 2, 150.0)
                ));

        // LEFT JOIN: users.id = orders.user_id
        NestedLoopJoinOperator join = new NestedLoopJoinOperator(
                leftScan,
                rightScan,
                NestedLoopJoinOperator.JoinType.LEFT,
                NestedLoopJoinOperator.equalsCondition("id", "user_id")
        );

        List<Map<String, Object>> results = executeOperator(join);

        // Alice 1条, Bob 1条, David 1条(order_id为null)
        assertEquals(3, results.size());

        // 验证 David 的记录（没有订单）
        Map<String, Object> davidRow = results.stream()
                .filter(row -> "David".equals(row.get("name")))
                .findFirst()
                .orElse(null);

        assertNotNull(davidRow);
        assertNull(davidRow.get("order_id")); // 右表列应该是 NULL

        System.out.println("\n=== LEFT JOIN (users LEFT JOIN orders) ===");
        printResults(results);
    }

    /**
     * 测试 JOIN + Filter + Project 组合
     */
    @Test
    @Order(6)
    void testJoinWithFilterAndProject() throws Exception {
        // 左表: users
        TableScanOperator leftScan = new TableScanOperator("users",
                Arrays.asList(
                        createUser(1, "Alice", 25),
                        createUser(2, "Bob", 30),
                        createUser(3, "Charlie", 25)
                ));

        // 右表: orders
        TableScanOperator rightScan = new TableScanOperator("orders",
                Arrays.asList(
                        createOrder(101, 1, 100.0),
                        createOrder(102, 1, 200.0),
                        createOrder(103, 2, 150.0),
                        createOrder(104, 3, 300.0)
                ));

        // 1. JOIN
        NestedLoopJoinOperator join = new NestedLoopJoinOperator(
                leftScan,
                rightScan,
                NestedLoopJoinOperator.JoinType.INNER,
                NestedLoopJoinOperator.equalsCondition("id", "user_id")
        );

        // 2. Filter: amount > 150
        FilterOperator filter = new FilterOperator(
                join,
                row -> (double) row.get("amount") > 150,
                "amount > 150"
        );

        // 3. Project: SELECT name, order_id, amount
        ProjectOperator project = new ProjectOperator(
                filter,
                Arrays.asList("name", "order_id", "amount")
        );

        List<Map<String, Object>> results = executeOperator(project);

        // Alice的订单102(200.0) 和 Charlie的订单104(300.0)
        assertEquals(2, results.size());

        // 验证投影后只有3列
        for (Map<String, Object> row : results) {
            assertEquals(3, row.size());
            assertTrue(row.containsKey("name"));
            assertTrue(row.containsKey("order_id"));
            assertTrue(row.containsKey("amount"));
        }

        System.out.println("\n=== JOIN + Filter + Project ===");
        printResults(results);
    }

    /**
     * 测试统计信息
     */
    @Test
    @Order(7)
    void testJoinStatistics() throws Exception {
        TableScanOperator leftScan = new TableScanOperator("users",
                Arrays.asList(
                        createUser(1, "Alice", 25),
                        createUser(2, "Bob", 30)
                ));

        TableScanOperator rightScan = new TableScanOperator("orders",
                Arrays.asList(
                        createOrder(101, 1, 100.0),
                        createOrder(102, 1, 200.0),
                        createOrder(103, 2, 150.0)
                ));

        NestedLoopJoinOperator join = new NestedLoopJoinOperator(
                leftScan,
                rightScan,
                NestedLoopJoinOperator.JoinType.INNER,
                NestedLoopJoinOperator.equalsCondition("id", "user_id")
        );

        executeOperator(join);

        // 验证统计信息
        assertEquals(2, join.getLeftScannedRows());  // 2个用户
        assertEquals(6, join.getRightScannedRows()); // 每个用户扫描3个订单
        assertEquals(3, join.getMatchedRows());      // 3条匹配

        System.out.println("\n=== JOIN Statistics ===");
        System.out.println("Left scanned: " + join.getLeftScannedRows());
        System.out.println("Right scanned: " + join.getRightScannedRows());
        System.out.println("Matched: " + join.getMatchedRows());
    }

    // ==================== 辅助方法 ====================

    private Map<String, Object> createUser(int id, String name, int age) {
        Map<String, Object> user = new HashMap<>();
        user.put("id", id);
        user.put("name", name);
        user.put("age", age);
        return user;
    }

    private Map<String, Object> createOrder(int orderId, int userId, double amount) {
        Map<String, Object> order = new HashMap<>();
        order.put("order_id", orderId);
        order.put("user_id", userId);
        order.put("amount", amount);
        return order;
    }

    private List<Map<String, Object>> executeOperator(Operator operator) throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();

        try {
            operator.open();

            Map<String, Object> row;
            while ((row = operator.next()) != null) {
                results.add(new HashMap<>(row));
            }

        } finally {
            operator.close();
        }

        return results;
    }

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
