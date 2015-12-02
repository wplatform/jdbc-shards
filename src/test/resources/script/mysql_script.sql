
CREATE TABLE IF NOT EXISTS `customers` (
  `customer_id` int(11) NOT NULL,
  `rand_id` int(11) DEFAULT NULL,
  `name` varchar(20) DEFAULT NULL,
  `customer_info` varchar(100) DEFAULT NULL,
  `birthdate` date DEFAULT NULL,
  PRIMARY KEY (`customer_id`),
  KEY (`birthdate`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `address` (
  `address_id` int(11) NOT NULL,
  `customer_id` int(11) DEFAULT NULL,
  `address_info` varchar(512) DEFAULT NULL,
  `zip_code` varchar(16) DEFAULT NULL,
  `phone_num` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`address_id`),
  KEY (`customer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE IF NOT EXISTS `orders` (
  `order_id` int(11) NOT NULL,
  `customer_id` int(11) NOT NULL,
  `order_info` varchar(218) DEFAULT NULL,
  `create_date` datetime NOT NULL,
  PRIMARY KEY (`order_id`),
  KEY (`customer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `order_items` (
  `item_id` int(11) NOT NULL,
  `order_id` int(11) NOT NULL,
  `item_info` varchar(218) DEFAULT NULL,
  `create_date` datetime NOT NULL,
  PRIMARY KEY (`order_id`),
  KEY (`create_date`),
  FOREIGN KEY (`order_id`) REFERENCES `orders` (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `order_status` (
  `status_id` int(11) NOT NULL,
  `order_id` int(11) NOT NULL,
  `order_status` int(2) DEFAULT NULL,
  `create_date` datetime NOT NULL,
  PRIMARY KEY (`order_id`),
  KEY (`create_date`),
  FOREIGN KEY (`order_id`) REFERENCES `orders` (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `product_category` (
  `product_category_id` int(11) NOT NULL,
  `order_id` int(11) NOT NULL,
  `category_info` int(2) DEFAULT NULL,
  `create_date` datetime NOT NULL,
  PRIMARY KEY (`product_category_id`),
  KEY (`create_date`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `product` (
  `product_id` int(11) NOT NULL,
  `product_category_id` int(11) NOT NULL,
  `product_name` int(2) DEFAULT NULL,
  `create_date` datetime NOT NULL,
  PRIMARY KEY (`product_id`),
  KEY (`create_date`),
  FOREIGN KEY (`product_category_id`) REFERENCES `product_category` (`product_category_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

