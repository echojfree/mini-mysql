package com.minidb.lock;

import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 死锁检测器的单元测试
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DeadlockDetectorTest {

    private DeadlockDetector detector;

    @BeforeEach
    void setUp() {
        detector = new DeadlockDetector();
    }

    /**
     * 测试添加等待关系
     */
    @Test
    @Order(1)
    void testAddWaitRelation() {
        detector.addWaitRelation(1L, 2L, "table:users");
        assertEquals(1, detector.getGraphSize());
    }

    /**
     * 测试移除等待关系
     */
    @Test
    @Order(2)
    void testRemoveWaitRelation() {
        detector.addWaitRelation(1L, 2L, "table:users");
        detector.removeWaitRelation(1L, 2L);
        assertEquals(0, detector.getGraphSize());
    }

    /**
     * 测试无死锁情况
     */
    @Test
    @Order(3)
    void testNoDeadlock() {
        // T1 → T2
        detector.addWaitRelation(1L, 2L, "row:users:1");

        List<Long> cycle = detector.detectDeadlock();
        assertNull(cycle, "Should not detect deadlock");
    }

    /**
     * 测试简单死锁：T1 → T2 → T1
     */
    @Test
    @Order(4)
    void testSimpleDeadlock() {
        // T1 → T2
        detector.addWaitRelation(1L, 2L, "row:users:1");
        // T2 → T1
        detector.addWaitRelation(2L, 1L, "row:users:2");

        List<Long> cycle = detector.detectDeadlock();
        assertNotNull(cycle, "Should detect deadlock");
        assertEquals(2, cycle.size());
        assertTrue(cycle.contains(1L));
        assertTrue(cycle.contains(2L));
    }

    /**
     * 测试三方死锁：T1 → T2 → T3 → T1
     */
    @Test
    @Order(5)
    void testThreeWayDeadlock() {
        // T1 → T2 → T3 → T1
        detector.addWaitRelation(1L, 2L, "row:users:1");
        detector.addWaitRelation(2L, 3L, "row:users:2");
        detector.addWaitRelation(3L, 1L, "row:users:3");

        List<Long> cycle = detector.detectDeadlock();
        assertNotNull(cycle, "Should detect deadlock");
        assertEquals(3, cycle.size());
        assertTrue(cycle.contains(1L));
        assertTrue(cycle.contains(2L));
        assertTrue(cycle.contains(3L));
    }

    /**
     * 测试复杂等待图但无死锁
     */
    @Test
    @Order(6)
    void testComplexGraphNoDeadlock() {
        // T1 → T2 → T3
        // T4 → T5
        detector.addWaitRelation(1L, 2L, "row:users:1");
        detector.addWaitRelation(2L, 3L, "row:users:2");
        detector.addWaitRelation(4L, 5L, "row:products:1");

        List<Long> cycle = detector.detectDeadlock();
        assertNull(cycle, "Should not detect deadlock");
    }

    /**
     * 测试部分环路死锁
     */
    @Test
    @Order(7)
    void testPartialCycleDeadlock() {
        // T1 → T2
        // T2 → T3 → T4 → T3 (环路在 T3-T4 之间)
        detector.addWaitRelation(1L, 2L, "row:users:1");
        detector.addWaitRelation(2L, 3L, "row:users:2");
        detector.addWaitRelation(3L, 4L, "row:users:3");
        detector.addWaitRelation(4L, 3L, "row:users:4");

        List<Long> cycle = detector.detectDeadlock();
        assertNotNull(cycle, "Should detect deadlock");
        assertTrue(cycle.contains(3L));
        assertTrue(cycle.contains(4L));
    }

    /**
     * 测试牺牲者选择：选择持有锁最少的事务
     */
    @Test
    @Order(8)
    void testVictimSelection_MinLocks() {
        // 注册事务元数据
        detector.registerTransaction(1L, 5);  // T1 持有 5 个锁
        detector.registerTransaction(2L, 2);  // T2 持有 2 个锁（应该被选为牺牲者）
        detector.registerTransaction(3L, 10); // T3 持有 10 个锁

        // 创建死锁环路
        detector.addWaitRelation(1L, 2L, "row:users:1");
        detector.addWaitRelation(2L, 3L, "row:users:2");
        detector.addWaitRelation(3L, 1L, "row:users:3");

        List<Long> cycle = detector.detectDeadlock();
        assertNotNull(cycle);

        long victim = detector.selectVictim(cycle);
        assertEquals(2L, victim, "Should select transaction with minimum locks");
    }

    /**
     * 测试移除事务后无死锁
     */
    @Test
    @Order(9)
    void testRemoveTransactionResolvesDeadlock() {
        // 创建死锁：T1 → T2 → T1
        detector.addWaitRelation(1L, 2L, "row:users:1");
        detector.addWaitRelation(2L, 1L, "row:users:2");

        // 确认有死锁
        List<Long> cycle = detector.detectDeadlock();
        assertNotNull(cycle);

        // 移除 T1
        detector.removeTransaction(1L);

        // 应该没有死锁了
        cycle = detector.detectDeadlock();
        assertNull(cycle, "Deadlock should be resolved after removing transaction");
    }

    /**
     * 测试清空等待图
     */
    @Test
    @Order(10)
    void testClearGraph() {
        detector.addWaitRelation(1L, 2L, "row:users:1");
        detector.addWaitRelation(2L, 3L, "row:users:2");
        detector.addWaitRelation(3L, 1L, "row:users:3");

        assertTrue(detector.getGraphSize() > 0);

        detector.clear();
        assertEquals(0, detector.getGraphSize());

        List<Long> cycle = detector.detectDeadlock();
        assertNull(cycle);
    }

    /**
     * 测试打印等待图
     */
    @Test
    @Order(11)
    void testPrintWaitForGraph() {
        detector.addWaitRelation(1L, 2L, "row:users:1");
        detector.addWaitRelation(2L, 3L, "row:users:2");

        // 应该不抛出异常
        assertDoesNotThrow(() -> detector.printWaitForGraph());
    }

    /**
     * 测试大规模死锁检测
     */
    @Test
    @Order(12)
    void testLargeScaleDeadlockDetection() {
        // 创建一个大环路：T1 → T2 → T3 → ... → T10 → T1
        for (long i = 1; i <= 10; i++) {
            long next = (i == 10) ? 1 : i + 1;
            detector.addWaitRelation(i, next, "row:users:" + i);
        }

        List<Long> cycle = detector.detectDeadlock();
        assertNotNull(cycle, "Should detect large deadlock cycle");
        assertEquals(10, cycle.size());
    }

    /**
     * 测试自环（事务等待自己）
     */
    @Test
    @Order(13)
    void testSelfCycle() {
        // T1 → T1 (自己等待自己)
        detector.addWaitRelation(1L, 1L, "row:users:1");

        List<Long> cycle = detector.detectDeadlock();
        assertNotNull(cycle, "Should detect self-cycle as deadlock");
        assertEquals(1, cycle.size());
        assertEquals(1L, cycle.get(0));
    }

    /**
     * 测试并发添加等待关系
     */
    @Test
    @Order(14)
    void testConcurrentAddWaitRelation() throws InterruptedException {
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final long txnId = i + 1;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 10; j++) {
                    detector.addWaitRelation(txnId, txnId + 100, "row:test:" + j);
                }
            });
            threads[i].start();
        }

        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }

        assertTrue(detector.getGraphSize() > 0);
    }
}
