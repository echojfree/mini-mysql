package com.minidb.log;

import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Binlog 和两阶段提交测试
 *
 * 测试内容:
 * 1. Binlog 记录的写入和读取
 * 2. Binlog 序列化和反序列化
 * 3. 两阶段提交流程
 * 4. 崩溃恢复
 *
 * @author Mini-MySQL
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BinlogAndTwoPhaseCommitTest {

    private static final String BINLOG_FILE = "test_binlog.log";
    private static final String REDO_LOG_FILE = "test_redo.log";

    private BinlogManager binlogManager;
    private RedoLogManager redoLogManager;
    private TwoPhaseCommitCoordinator coordinator;

    @BeforeEach
    void setUp() throws IOException {
        // 清理旧文件
        new File(BINLOG_FILE).delete();
        new File(REDO_LOG_FILE).delete();

        // 创建管理器
        binlogManager = new BinlogManager(BINLOG_FILE);
        redoLogManager = new RedoLogManager(REDO_LOG_FILE, 100);
        coordinator = new TwoPhaseCommitCoordinator(redoLogManager, binlogManager);
    }

    @AfterEach
    void tearDown() throws IOException {
        binlogManager.close();

        // 清理文件
        new File(BINLOG_FILE).delete();
        new File(REDO_LOG_FILE).delete();
    }

    /**
     * 测试 Binlog 记录创建
     */
    @Test
    @Order(1)
    void testBinlogRecordCreation() {
        BinlogRecord record = new BinlogRecord(
                1L,
                BinlogRecord.EventType.INSERT,
                "users",
                "INSERT INTO users VALUES (1, 'Alice')"
        );

        assertEquals(1L, record.getTxnId());
        assertEquals(BinlogRecord.EventType.INSERT, record.getEventType());
        assertEquals("users", record.getTableName());
        assertEquals("INSERT INTO users VALUES (1, 'Alice')", record.getSqlStatement());
        assertTrue(record.getTimestamp() > 0);
    }

    /**
     * 测试 Binlog 序列化和反序列化
     */
    @Test
    @Order(2)
    void testBinlogSerialization() {
        BinlogRecord original = new BinlogRecord(
                100L,
                BinlogRecord.EventType.UPDATE,
                "products",
                "UPDATE products SET price = 99.9 WHERE id = 1"
        );
        original.setLsn(123);

        // 序列化
        byte[] data = original.serialize();
        assertTrue(data.length > 0);

        // 反序列化
        BinlogRecord deserialized = BinlogRecord.deserialize(data);

        assertEquals(original.getTxnId(), deserialized.getTxnId());
        assertEquals(original.getEventType(), deserialized.getEventType());
        assertEquals(original.getTableName(), deserialized.getTableName());
        assertEquals(original.getSqlStatement(), deserialized.getSqlStatement());
        assertEquals(original.getLsn(), deserialized.getLsn());
    }

    /**
     * 测试 Binlog 写入和读取
     */
    @Test
    @Order(3)
    void testBinlogWriteAndRead() throws IOException {
        // 写入多条记录
        BinlogRecord record1 = new BinlogRecord(1L, BinlogRecord.EventType.BEGIN, null, "BEGIN");
        BinlogRecord record2 = new BinlogRecord(1L, BinlogRecord.EventType.INSERT, "users",
                "INSERT INTO users VALUES (1, 'Alice')");
        BinlogRecord record3 = new BinlogRecord(1L, BinlogRecord.EventType.COMMIT, null, "COMMIT");

        long lsn1 = binlogManager.append(record1);
        long lsn2 = binlogManager.append(record2);
        long lsn3 = binlogManager.append(record3);

        assertTrue(lsn1 > 0);
        assertTrue(lsn2 > lsn1);
        assertTrue(lsn3 > lsn2);

        // 刷盘
        binlogManager.flush();

        // 读取所有记录
        List<BinlogRecord> records = binlogManager.readAll();

        assertEquals(3, records.size());
        assertEquals(BinlogRecord.EventType.BEGIN, records.get(0).getEventType());
        assertEquals(BinlogRecord.EventType.INSERT, records.get(1).getEventType());
        assertEquals(BinlogRecord.EventType.COMMIT, records.get(2).getEventType());
    }

    /**
     * 测试按事务读取 Binlog
     */
    @Test
    @Order(4)
    void testReadByTransaction() throws IOException {
        // 写入多个事务的记录
        binlogManager.append(new BinlogRecord(1L, BinlogRecord.EventType.INSERT, "t1", "SQL1"));
        binlogManager.append(new BinlogRecord(2L, BinlogRecord.EventType.INSERT, "t2", "SQL2"));
        binlogManager.append(new BinlogRecord(1L, BinlogRecord.EventType.UPDATE, "t1", "SQL3"));
        binlogManager.append(new BinlogRecord(2L, BinlogRecord.EventType.DELETE, "t2", "SQL4"));
        binlogManager.flush();

        // 读取事务 1 的记录
        List<BinlogRecord> txn1Records = binlogManager.readByTransaction(1L);
        assertEquals(2, txn1Records.size());
        assertEquals("SQL1", txn1Records.get(0).getSqlStatement());
        assertEquals("SQL3", txn1Records.get(1).getSqlStatement());

        // 读取事务 2 的记录
        List<BinlogRecord> txn2Records = binlogManager.readByTransaction(2L);
        assertEquals(2, txn2Records.size());
        assertEquals("SQL2", txn2Records.get(0).getSqlStatement());
        assertEquals("SQL4", txn2Records.get(1).getSqlStatement());
    }

    /**
     * 测试两阶段提交 - 成功场景
     */
    @Test
    @Order(5)
    void testTwoPhaseCommitSuccess() throws IOException {
        long txnId = 100L;

        // 准备 Binlog 记录
        List<BinlogRecord> binlogRecords = new ArrayList<>();
        binlogRecords.add(new BinlogRecord(txnId, BinlogRecord.EventType.BEGIN, null, "BEGIN"));
        binlogRecords.add(new BinlogRecord(txnId, BinlogRecord.EventType.INSERT,
                "orders", "INSERT INTO orders VALUES (1, 100.0)"));
        binlogRecords.add(new BinlogRecord(txnId, BinlogRecord.EventType.COMMIT, null, "COMMIT"));

        // 执行两阶段提交
        coordinator.commit(txnId, binlogRecords);

        // 验证 Binlog 已写入
        List<BinlogRecord> allRecords = binlogManager.readAll();
        assertEquals(3, allRecords.size());

        // 验证事务 ID
        for (BinlogRecord record : allRecords) {
            assertEquals(txnId, record.getTxnId());
        }

        System.out.println("\n=== Two-Phase Commit Success ===");
        System.out.println(coordinator.getStats());
    }

    /**
     * 测试两阶段提交 - 多个事务
     */
    @Test
    @Order(6)
    void testMultipleTransactions() throws IOException {
        // 事务 1
        List<BinlogRecord> txn1Records = new ArrayList<>();
        txn1Records.add(new BinlogRecord(1L, BinlogRecord.EventType.BEGIN, null, "BEGIN"));
        txn1Records.add(new BinlogRecord(1L, BinlogRecord.EventType.INSERT,
                "users", "INSERT INTO users VALUES (1, 'Alice')"));
        txn1Records.add(new BinlogRecord(1L, BinlogRecord.EventType.COMMIT, null, "COMMIT"));

        // 事务 2
        List<BinlogRecord> txn2Records = new ArrayList<>();
        txn2Records.add(new BinlogRecord(2L, BinlogRecord.EventType.BEGIN, null, "BEGIN"));
        txn2Records.add(new BinlogRecord(2L, BinlogRecord.EventType.UPDATE,
                "users", "UPDATE users SET name = 'Bob' WHERE id = 1"));
        txn2Records.add(new BinlogRecord(2L, BinlogRecord.EventType.COMMIT, null, "COMMIT"));

        // 提交事务 1
        coordinator.commit(1L, txn1Records);

        // 提交事务 2
        coordinator.commit(2L, txn2Records);

        // 验证
        List<BinlogRecord> allRecords = binlogManager.readAll();
        assertEquals(6, allRecords.size());

        List<BinlogRecord> txn1FromBinlog = binlogManager.readByTransaction(1L);
        List<BinlogRecord> txn2FromBinlog = binlogManager.readByTransaction(2L);

        assertEquals(3, txn1FromBinlog.size());
        assertEquals(3, txn2FromBinlog.size());

        System.out.println("\n=== Multiple Transactions ===");
        System.out.println("Transaction 1 records: " + txn1FromBinlog.size());
        System.out.println("Transaction 2 records: " + txn2FromBinlog.size());
    }

    /**
     * 测试 Binlog 统计信息
     */
    @Test
    @Order(7)
    void testBinlogStats() throws IOException {
        // 写入一些记录
        for (int i = 0; i < 10; i++) {
            binlogManager.append(new BinlogRecord(
                    (long) i,
                    BinlogRecord.EventType.INSERT,
                    "test_table",
                    "INSERT INTO test_table VALUES (" + i + ")"
            ));
        }

        binlogManager.flush();

        assertEquals(10, binlogManager.getRecordCount());
        assertEquals(10, binlogManager.getCurrentLsn().get());

        System.out.println("\n=== Binlog Stats ===");
        System.out.println(binlogManager.getStats());
    }

    /**
     * 测试崩溃恢复
     */
    @Test
    @Order(8)
    void testCrashRecovery() throws IOException {
        // 写入一些 Binlog 记录
        List<BinlogRecord> records = new ArrayList<>();
        records.add(new BinlogRecord(1L, BinlogRecord.EventType.BEGIN, null, "BEGIN"));
        records.add(new BinlogRecord(1L, BinlogRecord.EventType.INSERT,
                "users", "INSERT INTO users VALUES (1, 'Alice')"));
        records.add(new BinlogRecord(1L, BinlogRecord.EventType.COMMIT, null, "COMMIT"));

        coordinator.commit(1L, records);

        // 模拟崩溃恢复
        coordinator.recover();

        // 验证恢复后 Binlog 仍然可读
        List<BinlogRecord> recoveredRecords = binlogManager.readAll();
        assertEquals(3, recoveredRecords.size());

        System.out.println("\n=== Crash Recovery ===");
        System.out.println("Recovered records: " + recoveredRecords.size());
    }

    /**
     * 测试 Binlog 事件类型
     */
    @Test
    @Order(9)
    void testBinlogEventTypes() throws IOException {
        BinlogRecord.EventType[] eventTypes = {
                BinlogRecord.EventType.BEGIN,
                BinlogRecord.EventType.INSERT,
                BinlogRecord.EventType.UPDATE,
                BinlogRecord.EventType.DELETE,
                BinlogRecord.EventType.COMMIT,
                BinlogRecord.EventType.ROLLBACK
        };

        for (int i = 0; i < eventTypes.length; i++) {
            binlogManager.append(new BinlogRecord(
                    (long) i,
                    eventTypes[i],
                    "test_table",
                    "SQL for " + eventTypes[i]
            ));
        }

        binlogManager.flush();

        List<BinlogRecord> records = binlogManager.readAll();
        assertEquals(eventTypes.length, records.size());

        for (int i = 0; i < eventTypes.length; i++) {
            assertEquals(eventTypes[i], records.get(i).getEventType());
        }
    }

    /**
     * 测试 Binlog 清空
     */
    @Test
    @Order(10)
    void testBinlogClear() throws IOException {
        // 写入一些记录
        binlogManager.append(new BinlogRecord(1L, BinlogRecord.EventType.INSERT,
                "test", "SQL"));
        binlogManager.flush();

        assertEquals(1, binlogManager.getRecordCount());

        // 清空
        binlogManager.clear();

        assertEquals(0, binlogManager.getRecordCount());
        assertEquals(0, binlogManager.getCurrentLsn().get());

        List<BinlogRecord> records = binlogManager.readAll();
        assertEquals(0, records.size());
    }
}
