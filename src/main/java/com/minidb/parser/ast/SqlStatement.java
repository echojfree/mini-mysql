package com.minidb.parser.ast;

/**
 * SQL 语句抽象基类
 *
 * 这是所有 SQL 语句的基类,采用访问者模式支持 AST 的遍历和处理
 *
 * @author Mini-MySQL
 */
public abstract class SqlStatement {

    /**
     * 访问者模式接口
     * 允许在不修改 AST 节点的情况下添加新操作
     *
     * @param <T> 返回类型
     */
    public interface Visitor<T> {
        T visit(SelectStatement statement);
        T visit(InsertStatement statement);
        T visit(UpdateStatement statement);
        T visit(DeleteStatement statement);
        T visit(CreateTableStatement statement);
        T visit(DropTableStatement statement);
    }

    /**
     * 接受访问者
     *
     * @param visitor 访问者对象
     * @param <T>     返回类型
     * @return 访问结果
     */
    public abstract <T> T accept(Visitor<T> visitor);

    /**
     * 获取语句类型名称
     *
     * @return 语句类型
     */
    public String getStatementType() {
        return this.getClass().getSimpleName();
    }
}
