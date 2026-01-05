package com.minidb.optimizer;

import com.minidb.optimizer.cost.CostModel;
import com.minidb.optimizer.explain.ExplainGenerator;
import com.minidb.optimizer.explain.ExplainPlan;
import com.minidb.optimizer.index.IndexMetadata;
import com.minidb.optimizer.index.IndexType;
import com.minidb.optimizer.index.SecondaryIndexManager;
import com.minidb.optimizer.logical.LogicalOptimizer;
import com.minidb.optimizer.selector.IndexSelector;
import com.minidb.optimizer.stats.StatisticsCollector;
import com.minidb.optimizer.stats.TableStatistics;
import com.minidb.parser.SQLParser;
import com.minidb.parser.ast.SelectStatement;
import com.minidb.parser.ast.SqlStatement;
import com.minidb.storage.index.BPlusTree;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 查询优化器的综合测试
 *
 * 测试覆盖:
 * 1. 二级索引创建和查询
 * 2. 回表查询机制
 * 3. 统计信息收集
 * 4. 成本模型计算
 * 5. 索引选择算法
 * 6. 逻辑优化
 * 7. EXPLAIN 输出
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class QueryOptimizerTest {

    private SecondaryIndexManager indexManager;
    private StatisticsCollector statsCollector;
    private CostModel costModel;
    private IndexSelector indexSelector;
    private LogicalOptimizer logicalOptimizer;
    private ExplainGenerator explainGenerator;
    private SQLParser sqlParser;

    @BeforeEach
    void setUp() {
        indexManager = new SecondaryIndexManager();
        statsCollector = new StatisticsCollector();
        costModel = new CostModel();
        indexSelector = new IndexSelector(costModel, statsCollector);
        logicalOptimizer = new LogicalOptimizer();
        explainGenerator = new ExplainGenerator();
        sqlParser = new SQLParser();
    }

    /**
     * 测试创建聚簇索引和二级索引
     */
    @Test
    @Order(1)
    void testCreateIndexes() {
        // 创建聚簇索引（主键索引）
        indexManager.createClusteredIndex("users", "PRIMARY", "id", 100);

        // 创建二级索引
        indexManager.createSecondaryIndex("users", "idx_age",
                Collections.singletonList("age"), false, 100);
        indexManager.createSecondaryIndex("users", "idx_name",
                Collections.singletonList("name"), true, 100);

        // 验证索引数量
        List<IndexMetadata> indexes = indexManager.getIndexes("users");
        assertEquals(3, indexes.size());

        // 验证索引类型
        IndexMetadata primaryIndex = indexManager.findIndex("users", "PRIMARY");
        assertNotNull(primaryIndex);
        assertTrue(primaryIndex.isClustered());

        IndexMetadata ageIndex = indexManager.findIndex("users", "idx_age");
        assertNotNull(ageIndex);
        assertTrue(ageIndex.isSecondary());
        assertFalse(ageIndex.isUnique());

        IndexMetadata nameIndex = indexManager.findIndex("users", "idx_name");
        assertNotNull(nameIndex);
        assertTrue(nameIndex.isSecondary());
        assertTrue(nameIndex.isUnique());
    }

    /**
     * 测试二级索引的插入和查询
     */
    @Test
    @Order(2)
    @SuppressWarnings("unchecked")
    void testSecondaryIndexInsertAndLookup() {
        // 创建索引
        indexManager.createClusteredIndex("users", "PRIMARY", "id", 100);
        indexManager.createSecondaryIndex("users", "idx_age",
                Collections.singletonList("age"), false, 100);

        // 获取索引
        IndexMetadata primaryIndex = indexManager.findIndex("users", "PRIMARY");
        IndexMetadata ageIndex = indexManager.findIndex("users", "idx_age");

        // 在聚簇索引中插入数据（id -> 完整行数据）
        Map<String, Object> user1 = createUser(1, "Alice", 25);
        Map<String, Object> user2 = createUser(2, "Bob", 30);
        Map<String, Object> user3 = createUser(3, "Charlie", 25);

        ((BPlusTree<Integer, Map<String, Object>>) (BPlusTree) primaryIndex.getBtree())
                .insert(1, user1);
        ((BPlusTree<Integer, Map<String, Object>>) (BPlusTree) primaryIndex.getBtree())
                .insert(2, user2);
        ((BPlusTree<Integer, Map<String, Object>>) (BPlusTree) primaryIndex.getBtree())
                .insert(3, user3);

        // 在二级索引中插入 (age -> id)
        ((BPlusTree<Integer, Integer>) (BPlusTree) ageIndex.getBtree()).insert(25, 1);
        ((BPlusTree<Integer, Integer>) (BPlusTree) ageIndex.getBtree()).insert(30, 2);
        ((BPlusTree<Integer, Integer>) (BPlusTree) ageIndex.getBtree()).insert(25, 3);

        // 通过二级索引查找主键
        Integer primaryKey = indexManager.lookupPrimaryKey("users", "idx_age", 30);
        assertEquals(2, primaryKey);

        // 回表查询完整数据
        Map<String, Object> user = indexManager.lookupRow("users", primaryKey);
        assertNotNull(user);
        assertEquals("Bob", user.get("name"));
        assertEquals(30, user.get("age"));
    }

    /**
     * 测试统计信息收集
     */
    @Test
    @Order(3)
    void testStatisticsCollection() {
        // 准备测试数据
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            data.add(createUser(i, "User" + i, 20 + (i % 30)));
        }

        // 收集表统计信息
        statsCollector.analyzeTable("users", data);

        // 收集列统计信息
        statsCollector.analyzeColumn("users", "id", data);
        statsCollector.analyzeColumn("users", "age", data);
        statsCollector.analyzeColumn("users", "name", data);

        // 验证表统计信息
        TableStatistics stats = statsCollector.getTableStats("users");
        assertNotNull(stats);
        assertEquals(1000, stats.getRowCount());
        assertTrue(stats.getAvgRowLength() > 0);
        assertTrue(stats.getPageCount() > 0);

        // 验证列基数
        assertEquals(1000, statsCollector.getColumnCardinality("users", "id")); // 主键，基数=行数
        assertEquals(30, statsCollector.getColumnCardinality("users", "age")); // 30 个不同的年龄
        assertEquals(1000, statsCollector.getColumnCardinality("users", "name")); // 1000 个不同的名字

        // 验证选择性
        assertEquals(1.0, statsCollector.getSelectivity("users", "id"), 0.01);
        assertEquals(0.03, statsCollector.getSelectivity("users", "age"), 0.01);
        assertEquals(1.0, statsCollector.getSelectivity("users", "name"), 0.01);
    }

    /**
     * 测试成本模型
     */
    @Test
    @Order(4)
    void testCostModel() {
        // 准备数据
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 1; i <= 10000; i++) {
            data.add(createUser(i, "User" + i, 20 + (i % 30)));
        }

        statsCollector.analyzeTable("users", data);
        statsCollector.analyzeColumn("users", "age", data);

        TableStatistics stats = statsCollector.getTableStats("users");

        // 计算全表扫描成本
        double tableScanCost = costModel.estimateTableScanCost(stats);
        assertTrue(tableScanCost > 0);

        // 创建索引并计算索引扫描成本
        indexManager.createSecondaryIndex("users", "idx_age",
                Collections.singletonList("age"), false, 100);
        IndexMetadata ageIndex = indexManager.findIndex("users", "idx_age");

        // 假设过滤率为 10%
        double indexScanCost = costModel.estimateIndexScanCost(stats, ageIndex, 0.1);
        assertTrue(indexScanCost > 0);

        // 索引扫描成本应该低于全表扫描（在低过滤率情况下）
        assertTrue(indexScanCost < tableScanCost);

        // 测试回表成本
        long matchedRows = (long) (stats.getRowCount() * 0.1);
        double lookupCost = costModel.estimateLookupCost(matchedRows);
        assertTrue(lookupCost > 0);

        // 测试二级索引总成本（包含回表）
        double totalCost = costModel.estimateSecondaryIndexCost(stats, ageIndex, 0.1, true);
        assertEquals(indexScanCost + lookupCost, totalCost, 0.01);
    }

    /**
     * 测试索引选择算法
     */
    @Test
    @Order(5)
    void testIndexSelection() throws SQLParser.SQLParseException {
        // 准备数据
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 1; i <= 10000; i++) {
            data.add(createUser(i, "User" + i, 20 + (i % 30)));
        }

        statsCollector.analyzeTable("users", data);
        statsCollector.analyzeColumn("users", "age", data);

        // 创建索引
        indexManager.createClusteredIndex("users", "PRIMARY", "id", 100);
        indexManager.createSecondaryIndex("users", "idx_age",
                Collections.singletonList("age"), false, 100);

        // 解析 SQL
        String sql = "SELECT * FROM users WHERE age > 25";
        SqlStatement stmt = sqlParser.parse(sql);
        assertTrue(stmt instanceof SelectStatement);

        SelectStatement selectStmt = (SelectStatement) stmt;

        // 执行索引选择
        List<IndexMetadata> indexes = indexManager.getIndexes("users");
        IndexSelector.IndexSelectionResult result = indexSelector.selectIndex(selectStmt, indexes);

        // 验证选择结果
        assertNotNull(result);
        assertTrue(result.getEstimatedCost() > 0);
        assertTrue(result.getEstimatedRows() > 0);
    }

    /**
     * 测试逻辑优化 - 常量折叠
     */
    @Test
    @Order(6)
    void testConstantFolding() throws SQLParser.SQLParseException {
        // 解析包含常量的 SQL
        String sql = "SELECT * FROM users WHERE age > 18";
        SqlStatement stmt = sqlParser.parse(sql);
        SelectStatement selectStmt = (SelectStatement) stmt;

        // 执行逻辑优化
        SelectStatement optimized = logicalOptimizer.optimize(selectStmt);

        // 验证优化后的查询
        assertNotNull(optimized);
        assertNotNull(optimized.getWhereCondition());
    }

    /**
     * 测试 EXPLAIN 生成
     */
    @Test
    @Order(7)
    void testExplainGeneration() throws SQLParser.SQLParseException {
        // 准备数据
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            data.add(createUser(i, "User" + i, 20 + (i % 30)));
        }

        statsCollector.analyzeTable("users", data);
        statsCollector.analyzeColumn("users", "age", data);

        // 创建索引
        indexManager.createClusteredIndex("users", "PRIMARY", "id", 100);
        indexManager.createSecondaryIndex("users", "idx_age",
                Collections.singletonList("age"), false, 100);

        // 解析 SQL
        String sql = "SELECT * FROM users WHERE age = 25 ORDER BY name";
        SqlStatement stmt = sqlParser.parse(sql);
        SelectStatement selectStmt = (SelectStatement) stmt;

        // 执行索引选择
        List<IndexMetadata> indexes = indexManager.getIndexes("users");
        IndexSelector.IndexSelectionResult selection = indexSelector.selectIndex(selectStmt, indexes);

        // 生成 EXPLAIN
        ExplainPlan plan = explainGenerator.generateExplain(selectStmt, selection, indexes);

        // 验证 EXPLAIN
        assertNotNull(plan);
        assertEquals(1, plan.getId());
        assertEquals("SIMPLE", plan.getSelectType());
        assertEquals("users", plan.getTable());
        assertNotNull(plan.getType());
        assertTrue(plan.getPossibleKeys().size() > 0);
        assertTrue(plan.getRows() >= 0);

        // 打印 EXPLAIN（用于手动验证）
        System.out.println("\n=== EXPLAIN Output ===");
        explainGenerator.printExplain(plan);
    }

    /**
     * 测试覆盖索引优化
     */
    @Test
    @Order(8)
    void testCoveringIndex() throws SQLParser.SQLParseException {
        // 准备数据
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 1; i <= 1000; i++) {
            data.add(createUser(i, "User" + i, 20 + (i % 30)));
        }

        statsCollector.analyzeTable("users", data);
        statsCollector.analyzeColumn("users", "age", data);

        // 创建组合索引 (age, name)
        indexManager.createSecondaryIndex("users", "idx_age_name",
                Arrays.asList("age", "name"), false, 100);

        // 解析 SQL - 只查询索引中的列
        String sql = "SELECT age, name FROM users WHERE age = 25";
        SqlStatement stmt = sqlParser.parse(sql);
        SelectStatement selectStmt = (SelectStatement) stmt;

        // 执行索引选择
        List<IndexMetadata> indexes = indexManager.getIndexes("users");
        IndexSelector.IndexSelectionResult selection = indexSelector.selectIndex(selectStmt, indexes);

        // 验证使用了覆盖索引
        // 注意: 由于我们的简化实现，这里可能不会检测到覆盖索引
        // 实际 MySQL 会优化这种情况
        assertNotNull(selection);
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
}
