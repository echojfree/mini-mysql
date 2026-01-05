package com.minidb.executor;

import java.util.Map;

/**
 * 执行器算子接口 - 火山模型（Volcano Model）
 *
 * 火山模型是数据库执行引擎的经典设计，采用迭代器模式
 *
 * 核心思想:
 * - 每个算子都是一个迭代器
 * - 通过 next() 方法逐行返回数据
 * - 算子可以嵌套组合形成执行计划树
 *
 * 执行流程:
 * 1. open(): 初始化算子，打开资源
 * 2. next(): 返回下一行数据，如果没有更多数据返回 null
 * 3. close(): 关闭算子，释放资源
 *
 * 对应八股文知识点:
 * ✅ 什么是火山模型
 * ✅ 迭代器模式在数据库中的应用
 * ✅ SQL 执行引擎的实现原理
 * ✅ 算子之间如何传递数据
 *
 * @author Mini-MySQL
 */
public interface Operator {

    /**
     * 初始化算子
     *
     * 功能:
     * - 打开文件、连接等资源
     * - 初始化子算子
     * - 准备执行环境
     *
     * 八股文: 火山模型的第一步，必须先调用 open() 才能调用 next()
     *
     * @throws Exception 初始化失败时抛出
     */
    void open() throws Exception;

    /**
     * 获取下一行数据
     *
     * 功能:
     * - 返回一行数据（Map 格式，列名 -> 值）
     * - 如果没有更多数据，返回 null
     * - 采用拉取模式（Pull-based）
     *
     * 火山模型核心:
     * - 父算子通过调用子算子的 next() 获取数据
     * - 逐行处理，流式计算
     * - 节省内存（不需要一次性加载全部数据）
     *
     * 八股文:
     * - 为什么叫"火山模型"? 因为数据像火山喷发一样从底层算子流向上层算子
     * - 优点: 实现简单，内存占用低
     * - 缺点: 函数调用开销大（每行数据都要调用多次 next()）
     *
     * @return 下一行数据，如果没有返回 null
     * @throws Exception 执行失败时抛出
     */
    Map<String, Object> next() throws Exception;

    /**
     * 关闭算子
     *
     * 功能:
     * - 释放资源（文件、连接等）
     * - 关闭子算子
     * - 清理中间状态
     *
     * 八股文: 必须调用 close() 释放资源，避免资源泄漏
     *
     * @throws Exception 关闭失败时抛出
     */
    void close() throws Exception;

    /**
     * 获取算子类型（用于 EXPLAIN）
     *
     * @return 算子类型名称
     */
    default String getOperatorType() {
        return this.getClass().getSimpleName();
    }
}
