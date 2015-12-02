# jdbc-shards
jdbc-shards是基于关系型数据库集群实现的一个轻量级SQL引擎，致力于解决数据切分和切分后的数据查询问题。通过标准的JDBC API为上层应用提供数据访问服务。
# 为什么使用jdbc-shards
在数据量大和数据库读/写TPS高的背景下，通常的做是将数据进行切分，将原本存放在一个数据库上的数据按一定的规则切分到好几个或几组数据库中，这样的切分方式是基于(Share Nothing)的架构，各个数据库独立运行。从而，使的跨结点的join,count,order by,group by以及聚合函数等的SQL持性将无法得到支特，jdbc-shards致力于解决这些问题，使的在分布式的环境下访问多个数据库像访问一个数据库那样，对上层做到透明。
# 功能特点
- 嵌入式方式运行于应用中,提供标准JDBC API，无server模式，无需单独部署
- 分库分表，支持跨结点查询，支持join,count,order by,group by以及聚合函数等的SQL持性
- 读写分离支持，支持问题数据库隔离和自动恢复
- 对前端应用透明，屏蔽分布式数据库的复杂逻辑，使访问多个数据库像访问一个数据库那样
- 兼容主流的数据库MySql,PostgreSQL,Oracle,DB2,SQLServer的SQL语法
