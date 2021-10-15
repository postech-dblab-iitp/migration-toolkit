/*
create database migtestforhudson;
create user migtestforhudson identified by 'migtestforhudson';
grant all privileges on migtestforhudson.* to migtestforhudson@"%" identified by "migtestforhudson";
grant select on mysql.proc to migtestforhudson@"%" identified by "migtestforhudson";
*/
CREATE TABLE athlete(
`code` INT AUTO_INCREMENT,
`name` VARCHAR(40) NOT NULL,
gender CHAR(1),
nation_code CHAR(3),
event VARCHAR(30),
CONSTRAINT pk_athlete_code PRIMARY KEY(`code`)
)ENGINE=INNODB;

CREATE TABLE `code`(
s_name CHAR(1),
f_name VARCHAR(6)
)ENGINE=INNODB;

CREATE TABLE event(
`code` INT,
sports VARCHAR(50),
`name` VARCHAR(50),
gender CHAR(1),
players INT,
CONSTRAINT pk_event_code PRIMARY KEY(`code`)
)ENGINE=INNODB;

CREATE TABLE game(
host_year INT NOT NULL,
event_code INT NOT NULL,
athlete_code INT NOT NULL,
stadium_code INT NOT NULL,
nation_code CHAR(3),
medal CHAR(1),
game_date DATE,
CONSTRAINT pk_game_host_year_event_code_athlete_code PRIMARY KEY(host_year,event_code,athlete_code)
)ENGINE=INNODB;

CREATE TABLE history(
event_code INT NOT NULL,
athlete VARCHAR(40) NOT NULL,
host_year INT,
score VARCHAR(10),
unit VARCHAR(5),
CONSTRAINT pk_history_event_code_athlete PRIMARY KEY(event_code,athlete)
)ENGINE=INNODB;

CREATE TABLE nation(
`code` CHAR(3),
`name` VARCHAR(40) NOT NULL,
continent VARCHAR(10),
capital VARCHAR(30),
CONSTRAINT pk_nation_code PRIMARY KEY(`code`)
)ENGINE=INNODB;

CREATE TABLE olympic(
host_year INT,
host_nation VARCHAR(40) NOT NULL,
host_city VARCHAR(20) NOT NULL,
opening_date DATE NOT NULL,
closing_date DATE NOT NULL,
mascot VARCHAR(20),
slogan VARCHAR(40),
introduction VARCHAR(1500),
CONSTRAINT pk_olympic_host_year PRIMARY KEY(host_year)
)ENGINE=INNODB;

CREATE TABLE participant(
host_year INT NOT NULL,
nation_code CHAR(3) NOT NULL,
gold INT DEFAULT 0,
silver INT DEFAULT 0,
bronze INT DEFAULT 0,
CONSTRAINT pk_participant_host_year_nation_code PRIMARY KEY(host_year,nation_code)
)ENGINE=INNODB;

CREATE TABLE record(
host_year INT NOT NULL,
event_code INT NOT NULL,
athlete_code INT NOT NULL,
medal CHAR(1) NOT NULL,
score VARCHAR(20),
unit VARCHAR(5),
CONSTRAINT pk_record_host_year_event_code_athlete_code_medal PRIMARY KEY(host_year,event_code,athlete_code,medal)
)ENGINE=INNODB;

CREATE TABLE stadium(
`code` INT,
nation_code CHAR(3) NOT NULL,
`name` VARCHAR(50) NOT NULL,
`area` NUMERIC(10,2),
seats INT,
address VARCHAR(100),
CONSTRAINT pk_stadium_code PRIMARY KEY(`code`)
)ENGINE=INNODB;

ALTER TABLE game ADD CONSTRAINT fk_game_event_code FOREIGN KEY (event_code) REFERENCES event(`code`) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE game ADD CONSTRAINT fk_game_athlete_code FOREIGN KEY (athlete_code) REFERENCES athlete(`code`) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE participant ADD CONSTRAINT fk_participant_host_year FOREIGN KEY (host_year) REFERENCES olympic(host_year) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE participant ADD CONSTRAINT fk_participant_nation_code FOREIGN KEY (nation_code) REFERENCES nation(`code`) ON DELETE RESTRICT ON UPDATE RESTRICT;

CREATE INDEX idx_athlete_name ON athlete(`name`);
CREATE INDEX idx_game_game_date ON game(game_date DESC);


CREATE OR REPLACE VIEW game_view
    AS 
SELECT a.* FROM game a, event b WHERE a.event_code=b.`code`;

CREATE OR REPLACE VIEW participant_view
    AS 
SELECT a.* FROM participant a ,nation b WHERE a.nation_code=b.`code`;


CREATE TABLE test_binary (
  f1 TINYBLOB,
  f2 BLOB,
  f3 MEDIUMBLOB,
  f4 LONGBLOB,
  f5 BINARY(255) DEFAULT NULL,
  f6 VARBINARY(60000) DEFAULT NULL,
  f7 BIT(64) DEFAULT NULL
) ENGINE=INNODB;

CREATE TABLE test_number (
  id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  f1 TINYINT(4) DEFAULT NULL,
  f2 SMALLINT(6) DEFAULT NULL,
  f3 MEDIUMINT(9) DEFAULT NULL,
  f4 INT(11) DEFAULT NULL,
  f5 BIGINT(20) DEFAULT NULL,
  f6 FLOAT DEFAULT NULL,
  f7 DOUBLE DEFAULT NULL,
  f8 DECIMAL(65,0) DEFAULT NULL,
  f9 DECIMAL(65,2) DEFAULT NULL,
  PRIMARY KEY  (id)
) ENGINE=INNODB;


CREATE TABLE test_string (
  id VARCHAR(128) NOT NULL,
  f1 CHAR(1) DEFAULT NULL,
  f2 CHAR(2) DEFAULT NULL,
  f3 CHAR(255) DEFAULT NULL,
  f4 VARCHAR(1) DEFAULT NULL,
  f5 VARCHAR(2) DEFAULT NULL,
  f6 VARCHAR(4096) DEFAULT NULL,
  f7 TINYTEXT,
  f8 TEXT,
  f9 MEDIUMTEXT,
  f10 LONGTEXT,
  PRIMARY KEY  (id)
) ENGINE=INNODB;

CREATE TABLE test_time (
  f1 DATE DEFAULT NULL,
  f2 TIME DEFAULT NULL,
  f3 YEAR(4) DEFAULT NULL,
  f4 DATETIME DEFAULT NULL,
  f5 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=INNODB;


CREATE TABLE TEST_PARTITION_1
(
  F1 INTEGER,
  F2 VARCHAR(20),
  PRIMARY KEY (f1)
)ENGINE=INNODB
PARTITION BY HASH (F1)
(
  PARTITION TEST_PARTITION_1_1,
  PARTITION TEST_PARTITION_1_2,
  PARTITION TEST_PARTITION_1_3,
  PARTITION TEST_PARTITION_1_4
);

CREATE TABLE TEST_PARTITION_2
(
  f1 INTEGER,
  f2 DATETIME,
   PRIMARY KEY (f1,f2)
)ENGINE=INNODB
PARTITION BY RANGE (TO_DAYS(f2))
(
  PARTITION TEST_PARTITION_2_1 VALUES LESS THAN (TO_DAYS('2011-01-01')),
  PARTITION TEST_PARTITION_2_2 VALUES LESS THAN (TO_DAYS('2012-01-01')),
  PARTITION TEST_PARTITION_2_3 VALUES LESS THAN (TO_DAYS('2013-01-01'))
    
);

CREATE TABLE TEST_PARTITION_3
(
  f1 INTEGER,
  f2 VARCHAR(100),
  PRIMARY KEY (f1)
)ENGINE=INNODB
PARTITION BY LIST (f1)
(
  PARTITION TEST_PARTITION_3_1 VALUES IN (1,2,3),
  PARTITION TEST_PARTITION_3_2 VALUES IN (4,5,6),
  PARTITION TEST_PARTITION_3_3 VALUES IN (7,8,9),
  PARTITION TEST_PARTITION_3_4 VALUES IN (10,11,12)
    
);

DELIMITER $$

CREATE
    TRIGGER `test_trigger` BEFORE INSERT ON `test_string` 
    FOR EACH ROW BEGIN
    END;
$$

DELIMITER ;

DELIMITER $$

CREATE FUNCTION `test_function`(s CHAR(20)) RETURNS CHAR(50) CHARSET latin1
BEGIN
	RETURN ('Hello World');
    END$$

DELIMITER ;

DELIMITER $$

CREATE PROCEDURE `test_procedure`()
BEGIN
    END$$

DELIMITER ;