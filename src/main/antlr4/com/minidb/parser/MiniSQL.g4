/**
 * Mini-MySQL SQL 语法文件
 *
 * 支持的 SQL 语句:
 * - SELECT: 查询语句
 * - INSERT: 插入语句
 * - UPDATE: 更新语句
 * - DELETE: 删除语句
 * - CREATE TABLE: 创建表
 * - DROP TABLE: 删除表
 *
 * 支持的子句:
 * - WHERE: 条件过滤
 * - ORDER BY: 排序
 * - LIMIT: 限制结果数量
 *
 * @author Mini-MySQL
 */
grammar MiniSQL;

// ============================================================
// 语法规则 (Parser Rules)
// ============================================================

/**
 * SQL 语句入口
 */
sqlStatement
    : selectStatement
    | insertStatement
    | updateStatement
    | deleteStatement
    | createTableStatement
    | dropTableStatement
    ;

/**
 * SELECT 语句
 * 语法: SELECT columns FROM table [WHERE condition] [ORDER BY ...] [LIMIT n]
 */
selectStatement
    : SELECT selectElements
      FROM tableName
      whereClause?
      orderByClause?
      limitClause?
    ;

/**
 * SELECT 的列列表
 */
selectElements
    : STAR                                  // SELECT *
    | selectElement (',' selectElement)*    // SELECT col1, col2, ...
    ;

/**
 * SELECT 的单个列
 */
selectElement
    : columnName (AS? alias)?               // column AS alias 或 column alias
    ;

/**
 * INSERT 语句
 * 语法: INSERT INTO table (col1, col2) VALUES (val1, val2)
 */
insertStatement
    : INSERT INTO tableName
      '(' columnName (',' columnName)* ')'
      VALUES
      '(' constant (',' constant)* ')'
    ;

/**
 * UPDATE 语句
 * 语法: UPDATE table SET col1=val1, col2=val2 [WHERE condition]
 */
updateStatement
    : UPDATE tableName
      SET assignmentList
      whereClause?
    ;

/**
 * 赋值列表
 */
assignmentList
    : assignment (',' assignment)*
    ;

/**
 * 单个赋值
 */
assignment
    : columnName '=' expression
    ;

/**
 * DELETE 语句
 * 语法: DELETE FROM table [WHERE condition]
 */
deleteStatement
    : DELETE FROM tableName whereClause?
    ;

/**
 * CREATE TABLE 语句
 * 语法: CREATE TABLE table_name (col1 type1, col2 type2, ...)
 */
createTableStatement
    : CREATE TABLE tableName
      '(' columnDefinition (',' columnDefinition)* ')'
    ;

/**
 * 列定义
 */
columnDefinition
    : columnName dataType columnConstraint*
    ;

/**
 * 数据类型
 */
dataType
    : INT                                   // INT
    | VARCHAR '(' INTEGER_LITERAL ')'      // VARCHAR(n)
    | TEXT                                  // TEXT
    | BIGINT                                // BIGINT
    | DECIMAL '(' INTEGER_LITERAL (',' INTEGER_LITERAL)? ')'  // DECIMAL(p,s)
    ;

/**
 * 列约束
 */
columnConstraint
    : PRIMARY KEY                           // PRIMARY KEY
    | NOT NULL_LITERAL                      // NOT NULL
    | AUTO_INCREMENT                        // AUTO_INCREMENT
    | DEFAULT constant                      // DEFAULT value
    ;

/**
 * DROP TABLE 语句
 */
dropTableStatement
    : DROP TABLE tableName
    ;

/**
 * WHERE 子句
 */
whereClause
    : WHERE expression
    ;

/**
 * ORDER BY 子句
 */
orderByClause
    : ORDER BY orderByElement (',' orderByElement)*
    ;

/**
 * ORDER BY 元素
 */
orderByElement
    : columnName (ASC | DESC)?
    ;

/**
 * LIMIT 子句
 */
limitClause
    : LIMIT INTEGER_LITERAL
    ;

/**
 * 表达式 (支持比较、逻辑运算)
 */
expression
    : expression AND expression             // 逻辑 AND
    | expression OR expression              // 逻辑 OR
    | NOT expression                        // 逻辑 NOT
    | predicate                             // 谓词
    | constant                              // 常量值 (用于 UPDATE SET)
    ;

/**
 * 谓词 (比较表达式)
 */
predicate
    : columnName comparisonOperator constant    // col = 'value'
    | columnName IS NULL_LITERAL                // col IS NULL
    | columnName IS NOT NULL_LITERAL            // col IS NOT NULL
    | columnName IN '(' constant (',' constant)* ')'  // col IN (val1, val2)
    | columnName BETWEEN constant AND constant  // col BETWEEN val1 AND val2
    | '(' expression ')'                        // (expression)
    ;

/**
 * 比较运算符
 */
comparisonOperator
    : '='
    | '<>'
    | '!='
    | '<'
    | '<='
    | '>'
    | '>='
    | LIKE
    ;

/**
 * 常量值
 */
constant
    : INTEGER_LITERAL                       // 123
    | STRING_LITERAL                        // 'hello'
    | NULL_LITERAL                          // NULL
    | TRUE                                  // TRUE
    | FALSE                                 // FALSE
    ;

/**
 * 表名
 */
tableName
    : IDENTIFIER
    ;

/**
 * 列名
 */
columnName
    : IDENTIFIER
    ;

/**
 * 别名
 */
alias
    : IDENTIFIER
    ;

// ============================================================
// 词法规则 (Lexer Rules)
// ============================================================

/**
 * SQL 关键字 (不区分大小写)
 */
SELECT      : S E L E C T ;
FROM        : F R O M ;
WHERE       : W H E R E ;
INSERT      : I N S E R T ;
INTO        : I N T O ;
VALUES      : V A L U E S ;
UPDATE      : U P D A T E ;
SET         : S E T ;
DELETE      : D E L E T E ;
CREATE      : C R E A T E ;
TABLE       : T A B L E ;
DROP        : D R O P ;
ORDER       : O R D E R ;
BY          : B Y ;
ASC         : A S C ;
DESC        : D E S C ;
LIMIT       : L I M I T ;
AND         : A N D ;
OR          : O R ;
NOT         : N O T ;
IN          : I N ;
BETWEEN     : B E T W E E N ;
LIKE        : L I K E ;
IS          : I S ;
NULL_LITERAL: N U L L ;
TRUE        : T R U E ;
FALSE       : F A L S E ;
AS          : A S ;
PRIMARY     : P R I M A R Y ;
KEY         : K E Y ;
AUTO_INCREMENT : A U T O '_' I N C R E M E N T ;
DEFAULT     : D E F A U L T ;

/**
 * 数据类型关键字
 */
INT         : I N T ;
VARCHAR     : V A R C H A R ;
TEXT        : T E X T ;
BIGINT      : B I G I N T ;
DECIMAL     : D E C I M A L ;

/**
 * 特殊符号
 */
STAR        : '*' ;

/**
 * 标识符 (表名、列名等)
 */
IDENTIFIER
    : [a-zA-Z_][a-zA-Z0-9_]*
    ;

/**
 * 整数字面量
 */
INTEGER_LITERAL
    : [0-9]+
    ;

/**
 * 字符串字面量
 */
STRING_LITERAL
    : '\'' (~['\r\n] | '\'\'')* '\''
    ;

/**
 * 空白字符 (忽略)
 */
WHITESPACE
    : [ \t\r\n]+ -> skip
    ;

/**
 * 单行注释 (忽略)
 */
LINE_COMMENT
    : '--' ~[\r\n]* -> skip
    ;

/**
 * 多行注释 (忽略)
 */
BLOCK_COMMENT
    : '/*' .*? '*/' -> skip
    ;

// ============================================================
// 辅助规则 (不区分大小写的字母匹配)
// ============================================================

fragment A : [aA] ;
fragment B : [bB] ;
fragment C : [cC] ;
fragment D : [dD] ;
fragment E : [eE] ;
fragment F : [fF] ;
fragment G : [gG] ;
fragment H : [hH] ;
fragment I : [iI] ;
fragment J : [jJ] ;
fragment K : [kK] ;
fragment L : [lL] ;
fragment M : [mM] ;
fragment N : [nN] ;
fragment O : [oO] ;
fragment P : [pP] ;
fragment Q : [qQ] ;
fragment R : [rR] ;
fragment S : [sS] ;
fragment T : [tT] ;
fragment U : [uU] ;
fragment V : [vV] ;
fragment W : [wW] ;
fragment X : [xX] ;
fragment Y : [yY] ;
fragment Z : [zZ] ;
