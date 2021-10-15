--CREATE USER migtestforhudson PROFILE "DEFAULT" IDENTIFIED BY migtestforhudson ACCOUNT UNLOCK;
--grant CONNECT,RESOURCE, create any trigger,create any view,create table,drop any view to migtestforhudson;

CREATE TABLE athlete(
code INT,
name VARCHAR2(40) NOT NULL,
gender CHAR(1),
nation_code CHAR(3),
event VARCHAR2(30),
 PRIMARY KEY(code)
);

CREATE TABLE code(
s_name CHAR(1),
f_name VARCHAR2(6)
);

CREATE TABLE event(
code INT,
sports VARCHAR2(50),
name VARCHAR2(50),
gender CHAR(1),
players INT,
 PRIMARY KEY(code)
);

CREATE TABLE game(
host_year INT NOT NULL,
event_code INT NOT NULL,
athlete_code INT NOT NULL,
stadium_code INT NOT NULL,
nation_code CHAR(3),
medal CHAR(1),
game_date DATE,
 PRIMARY KEY(host_year,event_code,athlete_code)
);

CREATE TABLE history(
event_code INT NOT NULL,
athlete VARCHAR2(40) NOT NULL,
host_year INT,
score VARCHAR2(10),
unit VARCHAR2(5),
 PRIMARY KEY(event_code,athlete)
);

CREATE TABLE nation(
code CHAR(3),
name VARCHAR2(40) NOT NULL,
continent VARCHAR2(10),
capital VARCHAR2(30),
 PRIMARY KEY(code)
);

CREATE TABLE olympic(
host_year INT,
host_nation VARCHAR2(40) NOT NULL,
host_city VARCHAR2(20) NOT NULL,
opening_date DATE NOT NULL,
closing_date DATE NOT NULL,
mascot VARCHAR2(20),
slogan VARCHAR2(40),
introduction VARCHAR2(1500),
 PRIMARY KEY(host_year)
);

CREATE TABLE participant(
host_year INT NOT NULL,
nation_code CHAR(3) NOT NULL,
gold INT DEFAULT 0,
silver INT DEFAULT 0,
bronze INT DEFAULT 0,
 PRIMARY KEY(host_year,nation_code)
);

CREATE TABLE record(
host_year INT NOT NULL,
event_code INT NOT NULL,
athlete_code INT NOT NULL,
medal CHAR(1) NOT NULL,
score VARCHAR2(20),
unit VARCHAR2(5),
 PRIMARY KEY(host_year,event_code,athlete_code,medal)
);

CREATE TABLE stadium(
code INT,
nation_code CHAR(3) NOT NULL,
name VARCHAR2(50) NOT NULL,
AREA NUMERIC(10,2),
seats INT,
address VARCHAR2(100),
 PRIMARY KEY(code)
);


ALTER TABLE game ADD (CONSTRAINT fk_game_event_code FOREIGN key (event_code) REFERENCES event(code) );
ALTER TABLE game ADD (CONSTRAINT fk_game_athlete_code FOREIGN key(athlete_code) REFERENCES athlete(code));
ALTER TABLE participant ADD (CONSTRAINT fk_participant_host_year FOREIGN key(host_year) REFERENCES olympic(host_year));
ALTER TABLE participant ADD (CONSTRAINT fk_participant_nation_code FOREIGN key(nation_code) REFERENCES nation(code));

CREATE INDEX idx_athlete_name ON athlete(name);
CREATE INDEX idx_game_game_date ON game(game_date DESC);


CREATE OR REPLACE VIEW game_view
    AS 
SELECT a.* FROM game a, event b WHERE a.event_code=b.code;

CREATE OR REPLACE VIEW participant_view
    AS 
SELECT a.* FROM participant a ,nation b WHERE a.nation_code=b.code;


CREATE TABLE test_binary (
  f1 BLOB,
  f2 bfile DEFAULT NULL,
  f3 long raw
) ;

CREATE TABLE test_number (
  id INT NOT NULL ,
  f1 SMALLINT DEFAULT NULL,
  f2 FLOAT(65) DEFAULT NULL,
  f3 DECIMAL(38,0) DEFAULT NULL,
  f4 DECIMAL(38,2) DEFAULT NULL,
  f5 NUMBER(38,0),
  f6 NUMBER,
  f7 REAL,
  PRIMARY KEY  (id)
) ;


CREATE TABLE test_string (
  id VARCHAR2(128) NOT NULL,
  f1 CHAR(1) DEFAULT NULL,
  f2 CHAR(2) DEFAULT NULL,
  f3 CHAR(255) DEFAULT NULL,
  f4 VARCHAR2(1) DEFAULT NULL,
  f5 VARCHAR2(2) DEFAULT NULL,
  f6 VARCHAR2(4000) DEFAULT NULL,
  f7 clob,
  f8 nclob,
  f9 long,
  PRIMARY KEY  (id)
) ;

create table TEST_TIME
(
  F1 DATE,
  F2 TIMESTAMP(6),
  F3 TIMESTAMP(9),
  F4 TIMESTAMP(6),
  F5 TIMESTAMP(9) WITH LOCAL TIME ZONE,
  F6 TIMESTAMP(9) WITH TIME ZONE,
  F7 TIMESTAMP(6) WITH LOCAL TIME ZONE,
  F8 TIMESTAMP(6) WITH TIME ZONE,
  F9  INTERVAL DAY(2) TO SECOND(9),
  F10 INTERVAL YEAR(4) TO MONTH
);

create table TEST_PARTITION_1
(
  F1 INTEGER,
  F2 VARCHAR2(20),
  primary key (f1)
)
partition by hash (F1)
(
  partition TEST_PARTITION_1_1,
  partition TEST_PARTITION_1_2,
  partition TEST_PARTITION_1_3,
  partition TEST_PARTITION_1_4
);

create table TEST_PARTITION_2
(
  f1 integer,
  f2 date,
   primary key (f1)
)
partition by range (f2)
(
  partition TEST_PARTITION_2_1 values less than (to_date('2011-01-01','yyyy-mm-dd')),
  partition TEST_PARTITION_2_2 values less than (to_date('2012-01-01','yyyy-mm-dd')),
  partition TEST_PARTITION_2_3 values less than (to_date('2013-01-01','yyyy-mm-dd'))
    
);

create table TEST_PARTITION_3
(
  f1 integer,
  f2 varchar2(100),
  primary key (f1)
)
partition by list (f2)
(
  partition TEST_PARTITION_3_1 values ('N'),
  partition TEST_PARTITION_3_2 values ('W'),
  partition TEST_PARTITION_3_3 values ('S'),
  partition TEST_PARTITION_3_4 values ('E')
    
);
CREATE SEQUENCE test_sequence 
INCREMENT BY 1
START WITH 1
NOMAXVALUE
NOCYCLE
CACHE 10; 