package com.minidb.executor.operator;

import com.minidb.executor.Operator;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * 嵌套循环连接算子
 *
 * 功能: 实现两个表的 JOIN 操作
 *
 * 工作原理（经典的嵌套循环算法）:
 * 1. 外层循环: 遍历左表（驱动表）
 * 2. 内层循环: 对每一行左表数据，遍历右表（被驱动表）
 * 3. 条件匹配: 检查连接条件是否满足
 * 4. 结果合并: 满足条件的行进行合并
 *
 * 对应八股文知识点:
 * ✅ JOIN 的执行过程
 * ✅ Nested Loop Join 原理
 * ✅ 驱动表的选择（小表驱动大表）
 * ✅ JOIN 的优化（使用索引）
 * ✅ Block Nested Loop 优化
 * ✅ JOIN Buffer 的作用
 *
 * @author Mini-MySQL
 */
@Slf4j
public class NestedLoopJoinOperator implements Operator {

    /**
     * JOIN 类型
     */
    public enum JoinType {
        INNER,      // 内连接
        LEFT,       // 左外连接
        RIGHT,      // 右外连接
        FULL        // 全外连接（简化版不支持）
    }

    /**
     * 连接条件
     */
    public interface JoinCondition {
        /**
         * 判断两行是否满足连接条件
         *
         * @param leftRow 左表行
         * @param rightRow 右表行
         * @return true 如果满足条件
         */
        boolean matches(Map<String, Object> leftRow, Map<String, Object> rightRow);
    }

    /**
     * 左表算子（驱动表）
     */
    private final Operator leftChild;

    /**
     * 右表算子（被驱动表）
     */
    private final Operator rightChild;

    /**
     * JOIN 类型
     */
    private final JoinType joinType;

    /**
     * 连接条件
     */
    private final JoinCondition condition;

    /**
     * 右表的物化数据
     * 为了避免重复扫描右表，将其物化到内存
     * 对应八股文: JOIN Buffer
     */
    private List<Map<String, Object>> rightTableData;

    /**
     * 当前左表行
     */
    private Map<String, Object> currentLeftRow;

    /**
     * 右表迭代器
     */
    private Iterator<Map<String, Object>> rightIterator;

    /**
     * 统计信息
     */
    private int leftScannedRows = 0;
    private int rightScannedRows = 0;
    private int matchedRows = 0;

    /**
     * 左外连接: 当前左表行是否已匹配
     */
    private boolean currentLeftMatched = false;

    /**
     * 构造函数
     *
     * @param leftChild 左表算子
     * @param rightChild 右表算子
     * @param joinType JOIN 类型
     * @param condition 连接条件
     */
    public NestedLoopJoinOperator(Operator leftChild, Operator rightChild,
                                 JoinType joinType, JoinCondition condition) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.joinType = joinType;
        this.condition = condition;
    }

    @Override
    public void open() throws Exception {
        log.debug("Opening NestedLoopJoin: type={}", joinType);

        // 打开左表
        leftChild.open();

        // 打开右表
        rightChild.open();

        // 物化右表数据（JOIN Buffer）
        // 对应八股文: 为什么需要 JOIN Buffer
        rightTableData = new ArrayList<>();
        Map<String, Object> rightRow;
        while ((rightRow = rightChild.next()) != null) {
            rightTableData.add(new HashMap<>(rightRow));
        }

        log.debug("Materialized right table: {} rows", rightTableData.size());

        // 初始化状态
        leftScannedRows = 0;
        rightScannedRows = 0;
        matchedRows = 0;
        currentLeftRow = null;
        rightIterator = null;
        currentLeftMatched = false;
    }

    @Override
    public Map<String, Object> next() throws Exception {
        while (true) {
            // 如果当前没有左表行，或右表已遍历完
            if (rightIterator == null || !rightIterator.hasNext()) {
                // 左外连接: 如果当前左表行没有匹配，返回左表行 + NULL
                if (joinType == JoinType.LEFT && currentLeftRow != null && !currentLeftMatched) {
                    log.trace("LEFT JOIN: no match for left row, returning with NULLs");
                    Map<String, Object> result = mergeRows(currentLeftRow, null);
                    currentLeftRow = null; // 移动到下一行
                    return result;
                }

                // 获取下一行左表数据
                currentLeftRow = leftChild.next();

                if (currentLeftRow == null) {
                    // 左表遍历完毕
                    log.debug("NestedLoopJoin finished: left={}, right={}, matched={}",
                            leftScannedRows, rightScannedRows, matchedRows);
                    return null;
                }

                leftScannedRows++;
                currentLeftMatched = false;

                // 重新开始遍历右表
                rightIterator = rightTableData.iterator();

                log.trace("Processing left row {}: {}", leftScannedRows, currentLeftRow);
            }

            // 遍历右表，寻找匹配
            while (rightIterator.hasNext()) {
                Map<String, Object> rightRow = rightIterator.next();
                rightScannedRows++;

                // 检查连接条件
                if (condition.matches(currentLeftRow, rightRow)) {
                    // 找到匹配
                    currentLeftMatched = true;
                    matchedRows++;

                    log.trace("Match found: left row {} with right row",
                            leftScannedRows);

                    return mergeRows(currentLeftRow, rightRow);
                }
            }

            // 右表遍历完，继续下一轮（获取下一行左表数据）
        }
    }

    @Override
    public void close() throws Exception {
        log.debug("Closing NestedLoopJoin");
        leftChild.close();
        rightChild.close();
        rightTableData = null;
    }

    @Override
    public String getOperatorType() {
        return "NestedLoopJoin(" + joinType + ")";
    }

    /**
     * 合并左右表的行数据
     *
     * @param leftRow 左表行
     * @param rightRow 右表行（可能为 null，用于左外连接）
     * @return 合并后的行
     */
    private Map<String, Object> mergeRows(Map<String, Object> leftRow,
                                         Map<String, Object> rightRow) {
        Map<String, Object> result = new HashMap<>();

        // 添加左表列（添加表前缀避免冲突）
        if (leftRow != null) {
            for (Map.Entry<String, Object> entry : leftRow.entrySet()) {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        // 添加右表列
        if (rightRow != null) {
            for (Map.Entry<String, Object> entry : rightRow.entrySet()) {
                // 如果列名冲突，右表列使用 "表名.列名" 格式
                String key = entry.getKey();
                if (result.containsKey(key)) {
                    key = "right_" + key; // 简化处理
                }
                result.put(key, entry.getValue());
            }
        } else {
            // 左外连接: 右表列填充 NULL
            // 这里简化处理，实际应该知道右表的列结构
            if (!rightTableData.isEmpty()) {
                Map<String, Object> sampleRight = rightTableData.get(0);
                for (String key : sampleRight.keySet()) {
                    if (!result.containsKey(key)) {
                        result.put(key, null);
                    } else {
                        result.put("right_" + key, null);
                    }
                }
            }
        }

        return result;
    }

    /**
     * 获取左表扫描行数
     */
    public int getLeftScannedRows() {
        return leftScannedRows;
    }

    /**
     * 获取右表扫描行数（总计，包括重复扫描）
     */
    public int getRightScannedRows() {
        return rightScannedRows;
    }

    /**
     * 获取匹配的行数
     */
    public int getMatchedRows() {
        return matchedRows;
    }

    /**
     * 创建等值连接条件
     *
     * @param leftColumn 左表列名
     * @param rightColumn 右表列名
     * @return 连接条件
     */
    public static JoinCondition equalsCondition(String leftColumn, String rightColumn) {
        return (leftRow, rightRow) -> {
            Object leftValue = leftRow.get(leftColumn);
            Object rightValue = rightRow.get(rightColumn);

            if (leftValue == null || rightValue == null) {
                return false;
            }

            return leftValue.equals(rightValue);
        };
    }
}
