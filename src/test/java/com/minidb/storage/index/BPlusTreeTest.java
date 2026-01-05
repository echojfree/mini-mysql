package com.minidb.storage.index;

import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * B+ 树的单元测试
 *
 * 测试用例覆盖：
 * 1. 基本插入和查询操作
 * 2. 节点分裂场景
 * 3. 范围查询功能
 * 4. 删除操作
 * 5. 边界条件测试
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BPlusTreeTest {

    private BPlusTree<Integer, String> tree;

    @BeforeEach
    void setUp() {
        // 使用较小的阶数便于测试节点分裂
        tree = new BPlusTree<>(5);
    }

    /**
     * 测试创建空树
     */
    @Test
    @Order(1)
    void testCreateEmptyTree() {
        assertTrue(tree.isEmpty());
        assertEquals(0, tree.getSize());
        assertEquals(1, tree.getHeight());
        assertNotNull(tree.getRoot());
        assertNull(tree.search(1));
    }

    /**
     * 测试插入单个元素
     */
    @Test
    @Order(2)
    void testInsertSingleElement() {
        tree.insert(10, "Value_10");

        assertFalse(tree.isEmpty());
        assertEquals(1, tree.getSize());
        assertEquals(1, tree.getHeight());
        assertEquals("Value_10", tree.search(10));
    }

    /**
     * 测试插入多个元素（不触发分裂）
     */
    @Test
    @Order(3)
    void testInsertMultipleElementsNoSplit() {
        tree.insert(10, "Value_10");
        tree.insert(20, "Value_20");
        tree.insert(30, "Value_30");
        tree.insert(40, "Value_40");

        assertEquals(4, tree.getSize());
        assertEquals(1, tree.getHeight());

        // 验证所有插入的值
        assertEquals("Value_10", tree.search(10));
        assertEquals("Value_20", tree.search(20));
        assertEquals("Value_30", tree.search(30));
        assertEquals("Value_40", tree.search(40));
    }

    /**
     * 测试插入触发叶子节点分裂
     */
    @Test
    @Order(4)
    void testInsertCausesLeafSplit() {
        // 阶数为 5，插入 6 个元素应该触发分裂
        tree.insert(10, "Value_10");
        tree.insert(20, "Value_20");
        tree.insert(30, "Value_30");
        tree.insert(40, "Value_40");
        tree.insert(50, "Value_50");
        tree.insert(60, "Value_60");

        assertEquals(6, tree.getSize());
        assertEquals(2, tree.getHeight()); // 分裂后高度增加

        // 验证所有值仍然可以查到
        assertEquals("Value_10", tree.search(10));
        assertEquals("Value_20", tree.search(20));
        assertEquals("Value_30", tree.search(30));
        assertEquals("Value_40", tree.search(40));
        assertEquals("Value_50", tree.search(50));
        assertEquals("Value_60", tree.search(60));
    }

    /**
     * 测试插入大量数据并验证
     */
    @Test
    @Order(5)
    void testInsertLargeDataSet() {
        // 插入 100 个元素
        for (int i = 1; i <= 100; i++) {
            tree.insert(i, "Value_" + i);
        }

        assertEquals(100, tree.getSize());
        assertTrue(tree.getHeight() >= 2); // 应该有多层

        // 随机验证一些值
        assertEquals("Value_1", tree.search(1));
        assertEquals("Value_50", tree.search(50));
        assertEquals("Value_100", tree.search(100));

        // 验证不存在的键
        assertNull(tree.search(101));
        assertNull(tree.search(0));
    }

    /**
     * 测试更新已存在的键
     */
    @Test
    @Order(6)
    void testUpdateExistingKey() {
        tree.insert(10, "Old_Value");
        assertEquals(1, tree.getSize());
        assertEquals("Old_Value", tree.search(10));

        // 更新值
        tree.insert(10, "New_Value");
        assertEquals(1, tree.getSize()); // 大小不变
        assertEquals("New_Value", tree.search(10));
    }

    /**
     * 测试乱序插入
     */
    @Test
    @Order(7)
    void testInsertInRandomOrder() {
        int[] keys = {50, 20, 80, 10, 30, 70, 90, 40, 60};

        for (int key : keys) {
            tree.insert(key, "Value_" + key);
        }

        assertEquals(keys.length, tree.getSize());

        // 验证所有值
        for (int key : keys) {
            assertEquals("Value_" + key, tree.search(key));
        }
    }

    /**
     * 测试范围查询（空结果）
     */
    @Test
    @Order(8)
    void testRangeSearchEmpty() {
        tree.insert(10, "Value_10");
        tree.insert(20, "Value_20");
        tree.insert(30, "Value_30");

        // 查询不存在的范围
        List<BPlusTree.Entry<Integer, String>> result = tree.rangeSearch(40, 50);
        assertTrue(result.isEmpty());
    }

    /**
     * 测试范围查询（单个结果）
     */
    @Test
    @Order(9)
    void testRangeSearchSingle() {
        tree.insert(10, "Value_10");
        tree.insert(20, "Value_20");
        tree.insert(30, "Value_30");

        List<BPlusTree.Entry<Integer, String>> result = tree.rangeSearch(20, 20);
        assertEquals(1, result.size());
        assertEquals(20, result.get(0).getKey().intValue());
        assertEquals("Value_20", result.get(0).getValue());
    }

    /**
     * 测试范围查询（多个结果）
     */
    @Test
    @Order(10)
    void testRangeSearchMultiple() {
        for (int i = 10; i <= 100; i += 10) {
            tree.insert(i, "Value_" + i);
        }

        // 查询 [30, 70] 范围
        List<BPlusTree.Entry<Integer, String>> result = tree.rangeSearch(30, 70);
        assertEquals(5, result.size());

        // 验证结果顺序和内容
        assertEquals(30, result.get(0).getKey().intValue());
        assertEquals(40, result.get(1).getKey().intValue());
        assertEquals(50, result.get(2).getKey().intValue());
        assertEquals(60, result.get(3).getKey().intValue());
        assertEquals(70, result.get(4).getKey().intValue());
    }

    /**
     * 测试范围查询（跨多个叶子节点）
     */
    @Test
    @Order(11)
    void testRangeSearchAcrossMultipleLeaves() {
        // 插入大量数据确保有多个叶子节点
        for (int i = 1; i <= 50; i++) {
            tree.insert(i, "Value_" + i);
        }

        // 查询大范围
        List<BPlusTree.Entry<Integer, String>> result = tree.rangeSearch(10, 40);
        assertEquals(31, result.size()); // 10 到 40 共 31 个数

        // 验证结果连续性
        for (int i = 0; i < result.size(); i++) {
            assertEquals(10 + i, result.get(i).getKey().intValue());
        }
    }

    /**
     * 测试删除操作（基本删除）
     */
    @Test
    @Order(12)
    void testDeleteBasic() {
        tree.insert(10, "Value_10");
        tree.insert(20, "Value_20");
        tree.insert(30, "Value_30");

        assertEquals(3, tree.getSize());

        // 删除中间元素
        String deleted = tree.delete(20);
        assertEquals("Value_20", deleted);
        assertEquals(2, tree.getSize());
        assertNull(tree.search(20));

        // 验证其他元素不受影响
        assertEquals("Value_10", tree.search(10));
        assertEquals("Value_30", tree.search(30));
    }

    /**
     * 测试删除不存在的键
     */
    @Test
    @Order(13)
    void testDeleteNonExistentKey() {
        tree.insert(10, "Value_10");

        // 删除不存在的键
        String deleted = tree.delete(20);
        assertNull(deleted);
        assertEquals(1, tree.getSize()); // 大小不变
    }

    /**
     * 测试删除所有元素
     */
    @Test
    @Order(14)
    void testDeleteAll() {
        tree.insert(10, "Value_10");
        tree.insert(20, "Value_20");
        tree.insert(30, "Value_30");

        tree.delete(10);
        tree.delete(20);
        tree.delete(30);

        assertTrue(tree.isEmpty());
        assertEquals(0, tree.getSize());
    }

    /**
     * 测试清空操作
     */
    @Test
    @Order(15)
    void testClear() {
        for (int i = 1; i <= 20; i++) {
            tree.insert(i, "Value_" + i);
        }

        assertEquals(20, tree.getSize());

        tree.clear();

        assertTrue(tree.isEmpty());
        assertEquals(0, tree.getSize());
        assertEquals(1, tree.getHeight());
        assertNull(tree.search(10));
    }

    /**
     * 测试字符串键的 B+ 树
     */
    @Test
    @Order(16)
    void testStringKeyTree() {
        BPlusTree<String, Integer> stringTree = new BPlusTree<>(5);

        stringTree.insert("apple", 1);
        stringTree.insert("banana", 2);
        stringTree.insert("cherry", 3);
        stringTree.insert("date", 4);

        assertEquals(4, stringTree.getSize());
        assertEquals(1, stringTree.search("apple").intValue());
        assertEquals(2, stringTree.search("banana").intValue());
        assertEquals(3, stringTree.search("cherry").intValue());
        assertEquals(4, stringTree.search("date").intValue());
    }

    /**
     * 测试异常情况：null 键插入
     */
    @Test
    @Order(17)
    void testInsertNullKey() {
        assertThrows(IllegalArgumentException.class, () -> {
            tree.insert(null, "Value");
        });
    }

    /**
     * 测试异常情况：null 键查询
     */
    @Test
    @Order(18)
    void testSearchNullKey() {
        assertThrows(IllegalArgumentException.class, () -> {
            tree.search(null);
        });
    }

    /**
     * 测试异常情况：null 键删除
     */
    @Test
    @Order(19)
    void testDeleteNullKey() {
        assertThrows(IllegalArgumentException.class, () -> {
            tree.delete(null);
        });
    }

    /**
     * 测试异常情况：范围查询参数 null
     */
    @Test
    @Order(20)
    void testRangeSearchNullKeys() {
        assertThrows(IllegalArgumentException.class, () -> {
            tree.rangeSearch(null, 100);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            tree.rangeSearch(1, null);
        });
    }

    /**
     * 测试异常情况：范围查询起始键大于结束键
     */
    @Test
    @Order(21)
    void testRangeSearchInvalidRange() {
        assertThrows(IllegalArgumentException.class, () -> {
            tree.rangeSearch(100, 50);
        });
    }

    /**
     * 测试异常情况：创建阶数过小的树
     */
    @Test
    @Order(22)
    void testCreateTreeWithInvalidOrder() {
        assertThrows(IllegalArgumentException.class, () -> {
            new BPlusTree<Integer, String>(2); // 阶数必须 >= 3
        });
    }

    /**
     * 测试大规模插入和查询性能
     */
    @Test
    @Order(23)
    void testLargeScalePerformance() {
        int count = 10000;
        long startTime = System.currentTimeMillis();

        // 插入 10000 个元素
        for (int i = 1; i <= count; i++) {
            tree.insert(i, "Value_" + i);
        }

        long insertTime = System.currentTimeMillis() - startTime;

        assertEquals(count, tree.getSize());

        // 查询性能测试
        startTime = System.currentTimeMillis();
        for (int i = 1; i <= count; i++) {
            assertNotNull(tree.search(i));
        }
        long searchTime = System.currentTimeMillis() - startTime;

        // 范围查询性能测试
        startTime = System.currentTimeMillis();
        List<BPlusTree.Entry<Integer, String>> result = tree.rangeSearch(1000, 2000);
        long rangeSearchTime = System.currentTimeMillis() - startTime;

        assertEquals(1001, result.size());

        // 输出性能数据
        System.out.println("=== B+ Tree Performance Test ===");
        System.out.println("Insert " + count + " elements: " + insertTime + " ms");
        System.out.println("Search " + count + " elements: " + searchTime + " ms");
        System.out.println("Range search (1000 results): " + rangeSearchTime + " ms");
        System.out.println("Tree height: " + tree.getHeight());
    }
}
