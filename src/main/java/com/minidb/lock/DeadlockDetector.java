package com.minidb.lock;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * 死锁检测器（DeadlockDetector）
 *
 * 使用等待图（Wait-For Graph）检测死锁
 *
 * 死锁的定义：
 * - 多个事务互相等待对方持有的锁，形成循环等待
 * - 例如：T1 等待 T2，T2 等待 T3，T3 等待 T1
 *
 * 等待图（Wait-For Graph）：
 * - 节点：事务
 * - 边：T1 → T2 表示 T1 等待 T2 持有的锁
 * - 如果图中存在环路，则发生死锁
 *
 * 死锁检测算法：
 * 1. 构建等待图
 * 2. 使用 DFS（深度优先搜索）检测环路
 * 3. 如果发现环路，选择一个事务回滚（牺牲者选择）
 *
 * 牺牲者选择策略：
 * 1. 优先选择持有锁较少的事务（代价较小）
 * 2. 优先选择运行时间较短的事务（撤销成本低）
 * 3. 优先选择优先级较低的事务
 *
 * 对应八股文知识点：
 * ✅ 什么是死锁？死锁的四个必要条件
 * ✅ 如何检测死锁？
 * ✅ 等待图（Wait-For Graph）是什么？
 * ✅ 死锁的牺牲者选择策略
 * ✅ 如何避免死锁？
 *
 * @author Mini-MySQL
 */
@Slf4j
public class DeadlockDetector {

    /**
     * 等待关系
     */
    @Data
    public static class WaitEdge {
        private long waiter;    // 等待者事务 ID
        private long holder;    // 持有者事务 ID
        private String resource; // 等待的资源

        public WaitEdge(long waiter, long holder, String resource) {
            this.waiter = waiter;
            this.holder = holder;
            this.resource = resource;
        }

        @Override
        public String toString() {
            return String.format("T%d → T%d (resource: %s)", waiter, holder, resource);
        }
    }

    /**
     * 等待图
     * Key: 事务 ID
     * Value: 该事务等待的所有事务 ID 列表
     */
    private final Map<Long, Set<Long>> waitForGraph;

    /**
     * 等待边的详细信息
     */
    private final List<WaitEdge> waitEdges;

    /**
     * 事务信息（用于牺牲者选择）
     * Key: 事务 ID
     * Value: 事务元数据（持有锁数量、运行时间等）
     */
    private final Map<Long, TransactionMetadata> transactionMetadata;

    /**
     * 事务元数据
     */
    @Data
    public static class TransactionMetadata {
        private long txnId;
        private int lockCount;      // 持有锁数量
        private long startTime;     // 事务开始时间
        private int priority;       // 优先级（数字越小优先级越高）

        public TransactionMetadata(long txnId) {
            this.txnId = txnId;
            this.lockCount = 0;
            this.startTime = System.currentTimeMillis();
            this.priority = 10; // 默认优先级
        }
    }

    /**
     * 构造函数
     */
    public DeadlockDetector() {
        this.waitForGraph = new HashMap<>();
        this.waitEdges = new ArrayList<>();
        this.transactionMetadata = new HashMap<>();

        log.info("DeadlockDetector initialized");
    }

    /**
     * 添加等待关系
     *
     * @param waiter   等待者事务 ID
     * @param holder   持有者事务 ID
     * @param resource 等待的资源
     */
    public synchronized void addWaitRelation(long waiter, long holder, String resource) {
        waitForGraph.computeIfAbsent(waiter, k -> new HashSet<>()).add(holder);
        waitEdges.add(new WaitEdge(waiter, holder, resource));

        log.debug("Added wait relation: T{} waits for T{} on resource '{}'", waiter, holder, resource);
    }

    /**
     * 移除等待关系
     *
     * @param waiter 等待者事务 ID
     * @param holder 持有者事务 ID
     */
    public synchronized void removeWaitRelation(long waiter, long holder) {
        Set<Long> waitSet = waitForGraph.get(waiter);
        if (waitSet != null) {
            waitSet.remove(holder);
            if (waitSet.isEmpty()) {
                waitForGraph.remove(waiter);
            }
        }

        waitEdges.removeIf(edge -> edge.getWaiter() == waiter && edge.getHolder() == holder);

        log.debug("Removed wait relation: T{} no longer waits for T{}", waiter, holder);
    }

    /**
     * 移除事务的所有等待关系
     *
     * @param txnId 事务 ID
     */
    public synchronized void removeTransaction(long txnId) {
        // 移除该事务作为等待者的关系
        waitForGraph.remove(txnId);

        // 移除该事务作为持有者的关系
        waitForGraph.values().forEach(waitSet -> waitSet.remove(txnId));

        // 移除等待边
        waitEdges.removeIf(edge -> edge.getWaiter() == txnId || edge.getHolder() == txnId);

        // 移除事务元数据
        transactionMetadata.remove(txnId);

        log.debug("Removed all wait relations for transaction T{}", txnId);
    }

    /**
     * 注册事务元数据
     *
     * @param txnId     事务 ID
     * @param lockCount 持有锁数量
     */
    public synchronized void registerTransaction(long txnId, int lockCount) {
        TransactionMetadata metadata = transactionMetadata.computeIfAbsent(txnId, TransactionMetadata::new);
        metadata.setLockCount(lockCount);
    }

    /**
     * 检测死锁
     *
     * @return 如果检测到死锁，返回参与死锁的事务列表；否则返回 null
     */
    public synchronized List<Long> detectDeadlock() {
        // 使用 DFS 检测环路
        Set<Long> visited = new HashSet<>();
        Set<Long> recursionStack = new HashSet<>();

        for (Long txnId : waitForGraph.keySet()) {
            if (!visited.contains(txnId)) {
                List<Long> cycle = dfs(txnId, visited, recursionStack, new ArrayList<>());
                if (cycle != null) {
                    log.warn("Deadlock detected! Cycle: {}", cycle);
                    return cycle;
                }
            }
        }

        return null; // 没有死锁
    }

    /**
     * DFS 检测环路
     *
     * @param txnId          当前事务 ID
     * @param visited        已访问的节点
     * @param recursionStack 递归栈（用于检测环路）
     * @param path           当前路径
     * @return 如果发现环路，返回环路中的事务列表；否则返回 null
     */
    private List<Long> dfs(long txnId, Set<Long> visited, Set<Long> recursionStack, List<Long> path) {
        visited.add(txnId);
        recursionStack.add(txnId);
        path.add(txnId);

        Set<Long> waitSet = waitForGraph.get(txnId);
        if (waitSet != null) {
            for (Long nextTxn : waitSet) {
                if (!visited.contains(nextTxn)) {
                    // 继续 DFS
                    List<Long> cycle = dfs(nextTxn, visited, recursionStack, path);
                    if (cycle != null) {
                        return cycle;
                    }
                } else if (recursionStack.contains(nextTxn)) {
                    // 发现环路！
                    int cycleStart = path.indexOf(nextTxn);
                    return new ArrayList<>(path.subList(cycleStart, path.size()));
                }
            }
        }

        recursionStack.remove(txnId);
        path.remove(path.size() - 1);
        return null;
    }

    /**
     * 选择牺牲者事务
     *
     * 策略：
     * 1. 持有锁最少的事务
     * 2. 运行时间最短的事务
     * 3. 优先级最低的事务
     *
     * @param cycle 死锁环路中的事务列表
     * @return 选择的牺牲者事务 ID
     */
    public synchronized long selectVictim(List<Long> cycle) {
        if (cycle == null || cycle.isEmpty()) {
            throw new IllegalArgumentException("Cycle cannot be empty");
        }

        long victim = cycle.get(0);
        int minScore = Integer.MAX_VALUE;

        for (Long txnId : cycle) {
            TransactionMetadata metadata = transactionMetadata.get(txnId);
            if (metadata == null) {
                metadata = new TransactionMetadata(txnId);
            }

            // 计算代价分数（越小越容易被选为牺牲者）
            // 分数 = 持有锁数量 * 10 + 运行时间(秒) + 优先级
            long runningTime = (System.currentTimeMillis() - metadata.getStartTime()) / 1000;
            int score = metadata.getLockCount() * 10 + (int) runningTime + metadata.getPriority();

            if (score < minScore) {
                minScore = score;
                victim = txnId;
            }
        }

        log.info("Selected victim transaction: T{} (score: {})", victim, minScore);
        return victim;
    }

    /**
     * 打印等待图
     */
    public synchronized void printWaitForGraph() {
        log.info("=== Wait-For Graph ===");
        if (waitForGraph.isEmpty()) {
            log.info("No waiting transactions");
        } else {
            for (Map.Entry<Long, Set<Long>> entry : waitForGraph.entrySet()) {
                log.info("T{} waits for: {}", entry.getKey(), entry.getValue());
            }
        }

        if (!waitEdges.isEmpty()) {
            log.info("Wait edges:");
            for (WaitEdge edge : waitEdges) {
                log.info("  {}", edge);
            }
        }
        log.info("=====================");
    }

    /**
     * 获取等待图大小（节点数量）
     *
     * @return 节点数量
     */
    public synchronized int getGraphSize() {
        return waitForGraph.size();
    }

    /**
     * 清空等待图
     */
    public synchronized void clear() {
        waitForGraph.clear();
        waitEdges.clear();
        transactionMetadata.clear();
        log.info("Wait-For Graph cleared");
    }
}
