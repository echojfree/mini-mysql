package com.minidb.optimizer.explain;

import com.minidb.optimizer.index.IndexMetadata;
import com.minidb.optimizer.selector.IndexSelector;
import com.minidb.parser.ast.SelectStatement;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * EXPLAIN 生成器
 *
 * 根据查询和索引选择结果，生成执行计划说明
 *
 * 对应八股文知识点:
 * ✅ EXPLAIN 的作用
 * ✅ 如何通过 EXPLAIN 优化查询
 * ✅ 哪些情况下会走索引
 * ✅ 覆盖索引的优势
 *
 * @author Mini-MySQL
 */
@Slf4j
public class ExplainGenerator {

    /**
     * 为 SELECT 查询生成 EXPLAIN 计划
     *
     * @param selectStmt SELECT 语句
     * @param selection 索引选择结果
     * @param availableIndexes 可用的索引列表
     * @return EXPLAIN 计划
     */
    public ExplainPlan generateExplain(SelectStatement selectStmt,
                                        IndexSelector.IndexSelectionResult selection,
                                        List<IndexMetadata> availableIndexes) {
        log.info("Generating EXPLAIN for table: {}", selectStmt.getTableName());

        ExplainPlan plan = new ExplainPlan();

        // 基本信息
        plan.setId(1);
        plan.setSelectType("SIMPLE");
        plan.setTable(selectStmt.getTableName());

        // 可能使用的索引
        for (IndexMetadata index : availableIndexes) {
            plan.addPossibleKey(index.getIndexName());
        }

        // 访问类型和索引信息
        switch (selection.getScanType()) {
            case TABLE_SCAN:
                plan.setType("ALL");
                plan.setKey(null);
                plan.addExtra("Using where");
                break;

            case CLUSTERED_INDEX_SCAN:
                plan.setType("index");
                plan.setKey(selection.getSelectedIndex().getIndexName());
                plan.setKeyColumnsUsed(selection.getSelectedIndex().getColumnNames().size());
                plan.addExtra("Using index");
                break;

            case SECONDARY_INDEX_SCAN:
                IndexMetadata index = selection.getSelectedIndex();

                // 根据查询条件确定访问类型
                if (index.isUnique()) {
                    plan.setType("eq_ref");
                } else {
                    plan.setType("ref");
                }

                plan.setKey(index.getIndexName());
                plan.setKeyColumnsUsed(index.getColumnNames().size());

                // 覆盖索引
                if (selection.isCoveringIndex()) {
                    plan.addExtra("Using index");
                } else if (selection.isNeedLookup()) {
                    plan.addExtra("Using index; Using where");
                } else {
                    plan.addExtra("Using where");
                }
                break;
        }

        // 估算信息
        plan.setRows(selection.getEstimatedRows());
        plan.setCost(selection.getEstimatedCost());

        // ORDER BY 处理
        if (selectStmt.getOrderByElements() != null &&
                !selectStmt.getOrderByElements().isEmpty()) {
            // 简化: 假设没有使用索引排序
            plan.addExtra("Using filesort");
        }

        // 过滤率（简化为 100%）
        plan.setFiltered(100.0);

        log.info("EXPLAIN plan generated: type={}, key={}, rows={}",
                plan.getType(), plan.getKey(), plan.getRows());

        return plan;
    }

    /**
     * 格式化并打印 EXPLAIN 结果
     *
     * @param plan EXPLAIN 计划
     */
    public void printExplain(ExplainPlan plan) {
        System.out.println("EXPLAIN Result:");
        System.out.println(ExplainPlan.tableSeparator());
        System.out.println(ExplainPlan.tableHeader());
        System.out.println(ExplainPlan.tableSeparator());
        System.out.println(plan.formatTable());
        System.out.println(ExplainPlan.tableSeparator());
        System.out.println();
        System.out.println("Detailed Info:");
        System.out.println(plan.format());
    }
}
