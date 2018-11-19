# Ted Moskovitz, 2018

# csvtables
CREATE TABLE `csvtables` (
  `t_name` varchar(64) NOT NULL,
  `csv_f` text,
  PRIMARY KEY (`t_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

# csvcolumns
CREATE TABLE `csvcolumns` (
  `column_name` varchar(64) NOT NULL,
  `column_type` text,
  `not_null` text,
  `t_name` varchar(64) NOT NULL,
  PRIMARY KEY (`column_name`,`t_name`),
  KEY `fk_col_to_table_idx` (`t_name`),
  CONSTRAINT `fk_col_to_table` FOREIGN KEY (`t_name`) REFERENCES `csvtables` (`t_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

# csvindexes
CREATE TABLE `csvindexes` (
  `index_name` varchar(64) NOT NULL,
  `index_type` text,
  `column_name` varchar(64) NOT NULL,
  `t_name` varchar(64) NOT NULL,
  `ordinal_pos` int(11) DEFAULT NULL,
  PRIMARY KEY (`index_name`,`t_name`,`column_name`),
  KEY `fk_index_to_table_idx` (`t_name`),
  KEY `fk_index_to_column_idx` (`column_name`),
  CONSTRAINT `fk_index_to_column` FOREIGN KEY (`column_name`) REFERENCES `csvcolumns` (`column_name`),
  CONSTRAINT `fk_index_to_table` FOREIGN KEY (`t_name`) REFERENCES `csvtables` (`t_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci