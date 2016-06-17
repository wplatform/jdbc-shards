
项目已更名为OpenDDAL,代码库移至[https://github.com/openddal/openddal](https://github.com/openddal/openddal)

# OpenDDAL概述
OpenDDAL致力于提供一个简单易用，高性能的分布式数据库访问服务，OpenDDAL内致的分布式SQL引擎能处理各种复杂的SQL，为之选择可靠的执行计划。OpenDDAL提供SQL语句执行计划查询，方便的理解SQL的路由及执行。有助于开发可靠的业务代码。除此之外，OpenDDAL提供了简洁高效的切分实现，数据库隔离及恢复，读写分离等功能。

# OpenDDAL架构
![](https://raw.githubusercontent.com/wplatform/blog/master/assets/openddal_main/architecture.png)


# OpenDDAL文档中心
- [功能演示](https://github.com/wplatform/blog/blob/master/posts/openddal-func-showcase.md)
- [用户指南](https://github.com/wplatform/blog/blob/master/posts/openddal-guide.md)
- [设计文档](https://github.com/wplatform/blog/blob/master/posts/openddal-design.md)

# OpenDDAL已实现的功能
- 简洁高效的数据切分方式，支持分库分表，自定义切分规则
- 基于JDBC，可适用于任何基于java的ORM框架
- 能处理各件复杂的SQL，对用户的使用限止少，对上层开发透明
- 实现基于成本的分布式SQL优化器，为SQL提供可靠的执行方式
- 支持SQL语句执行计划查询，方便的理解SQL的路由及执行
- 实现了基于MySQL数据库的Repository层
- 读写分离支持，HA支持，问题数据库隔离和自动恢复

# OpenDDAL需求规划
- 基于netty,整合cobar server.实现基于MySQL协议的TCP Server,以获多语言，多客户端支持。
- 实现更多的Repository层，如PgSQL，Oracle，DB2
- 实现基于NoSQL的Repository层，如Mongodb,Hbase 

