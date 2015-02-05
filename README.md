# 1、jdbc-shards是什么
基于jdbc api实现的分布式关系型数据库访问层，致力于解决数据切分，数据查询的问题。在数据量大和数据库读/写TPS高的背景下，通常的做是将数据进行切分，将原本存放在一个数据库上的数据按一定的规则切分到好几个或几组数据库中，这样的切分方式是基于(Share Nothing)的架构，各个数据库独立运行。从而，使的跨结点的join,count,order by,group by以及聚合函数等的SQL持性将无法得到支特，jdbc-shards致力于解决这些问题，它内置一个分布式查询引擎，使的在分布式的环境下访问多个数据库像访问一个数据库那样，对上层做到透明。

# 2、jdbc-shards功能特点
1. 小于1M的jar,基于jdbc实现，无server模式，无需单独部署
2. 支持分库分表，支持跨结点的join,count,order by,group by以及聚合函数等的SQL持性
3. 对前端应用透明，屏蔽分布式数据库的复杂逻辑，使访问多个数据库像访问一个数据库那样
4. 兼容主流的数据库MySql,Oracle,DB2,SQLServer,PostgreSQL的SQL语法
