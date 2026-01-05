# Mini MySQL

手写简化版 MySQL 数据库，用于深入理解数据库原理和八股文知识点。

## 项目目标

通过实现一个简化版的 MySQL 数据库，深入理解：
- 存储引擎原理（B+ 树、页式存储）
- 事务管理（MVCC、隔离级别）
- 锁机制（行锁、间隙锁、死锁检测）
- 日志系统（Undo Log、Redo Log、Binlog）
- 查询优化（索引选择、执行计划）

## 技术栈

- Java 11+
- Lombok
- Guava
- JUnit 5
- SLF4J + Logback

## 项目结构

```
mini-mysql/
├── src/
│   ├── main/
│   │   └── java/
│   │       └── com/
│   │           └── minidb/
│   │               ├── storage/          # 存储层
│   │               │   ├── page/         # 页面管理
│   │               │   ├── index/        # B+树索引
│   │               │   └── file/         # 文件I/O
│   │               ├── buffer/           # 缓冲池
│   │               ├── transaction/      # 事务管理
│   │               │   ├── mvcc/         # MVCC
│   │               │   ├── lock/         # 锁管理
│   │               │   └── log/          # Undo/Redo Log
│   │               ├── sql/              # SQL层
│   │               │   ├── parser/       # 解析器
│   │               │   ├── optimizer/    # 优化器
│   │               │   └── executor/     # 执行器
│   │               ├── network/          # 网络层
│   │               └── common/           # 通用工具
│   └── test/
│       └── java/
└── pom.xml
```

## 开发进度

详见 [plan.md](plan.md)

### 当前进度
- [x] 项目初始化
- [ ] 阶段 1: 基础存储引擎
- [ ] 阶段 2: 缓冲池管理
- [ ] 阶段 3: 事务基础
- [ ] 阶段 4: MVCC 和隔离级别
- [ ] 阶段 5: 锁机制
- [ ] 阶段 6: SQL 解析器
- [ ] 阶段 7: 查询优化器
- [ ] 阶段 8: 执行引擎
- [ ] 阶段 9: 网络协议
- [ ] 阶段 10: Binlog 和两阶段提交

## 构建和测试

```bash
# 编译项目
mvn clean compile

# 运行测试
mvn test

# 打包
mvn package
```

## 学习资源

- 《MySQL 技术内幕：InnoDB 存储引擎》
- 《高性能 MySQL》
- MySQL 官方文档

## 开源协议

MIT License
