CREATE TABLE IF NOT EXISTS `customers` (
  `customer_id` int(11) NOT NULL,
  `rand_id` int(11) DEFAULT NULL,
  `name` varchar(20) DEFAULT NULL,
  `customer_info` varchar(100) DEFAULT NULL,
  `birthdate` date DEFAULT NULL,
  PRIMARY KEY (`customer_id`),
  KEY `birthdate` (`birthdate`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE IF NOT EXISTS `orders` (
  `order_id` int(11) NOT NULL,
  `customer_id` int(11) NOT NULL,
  `order_info` varchar(200) DEFAULT NULL,
  `order_date` datetime NOT NULL,
  PRIMARY KEY (`order_id`),
  KEY `orders_fk` (`customer_id`),
  CONSTRAINT `orders_fk` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`customer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `data_types_test` (
  `col1` INT NOT NULL,
  `col2` VARCHAR(45) NULL,
  `col3` DECIMAL(4) NOT NULL,
  `col4` DATETIME NULL,
  `col5` BLOB NULL,
  `col6` DATE NULL,
  `col7` DOUBLE NULL,
  `col8` TINYINT(1) NOT NULL DEFAULT 0,
  `col9` SMALLINT(1) NULL,
  `col10` CHAR(10) NULL,
  `col11` NVARCHAR(10) NOT NULL DEFAULT 'test',
  `col12` YEAR NOT NULL DEFAULT 2001,
  `col13` BIGINT NULL,
  `col14` BINARY NULL,
  `col15` TINYBLOB NULL,
  `col16` MEDIUMBLOB NULL,
  `col17` LONGBLOB NULL,
  `col18` TINYTEXT NULL,
  `col19` LONGTEXT NULL,
  `col20` TEXT NULL,
  PRIMARY KEY (`col1`)
  ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE  `constraint_test_parent` (
  `col1 int not null` int(11) NOT NULL,
  `col2` int(11) DEFAULT NULL,
  `col3` varchar(20) DEFAULT NULL,
  `col4` varchar(200) DEFAULT NULL,
  `col5` int(1) DEFAULT 0,
  CONSTRAINT PRIMARY KEY (`col1`),
  CONSTRAINT UNIQUE KEY (`col2`),
  KEY USING BTREE (`col3`),
  CHECK (`col5` IN (1,2,3,4,5))

) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE  `constraint_test_child` (
  `col1` int(11) NOT NULL,
  `col2` int(11) NOT NULL,
  `col3` varchar(200) DEFAULT NULL,
  `col4` datetime NOT NULL,
  `col5` int(1) DEFAULT 0,
  CONSTRAINT PRIMARY KEY (`col1`),
   UNIQUE KEY (`col2`),
  CONSTRAINT FOREIGN KEY (`col3`) REFERENCES(`col4`),
  KEY(`col4`),
  CHECK (`col5` IN (1,2,3,4,5))
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE  test (   `col1 int not null` int(11) NOT NULL,   `col2` int(11) DEFAULT NULL,   `col3` varchar(20) DEFAULT NULL,   `col4` varchar(200) DEFAULT NULL,   `col5` int(1) DEFAULT 0,   PRIMARY KEY(`col1`),  UNIQUE KEY(`col2`),   KEY USING BTREE (`col3`),   CHECK (`col5` IN (1,2,3,4,5))  ) ENGINE=InnoDB DEFAULT CHARSET=latin1