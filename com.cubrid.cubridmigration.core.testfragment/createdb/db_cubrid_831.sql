--DROP TABLE code,game,history,participant,record,stadium,athlete,event,nation,olympic,test_binary,test_number,test_string,test_time;
CREATE TABLE athlete(
code INT AUTO_INCREMENT,
name VARCHAR(40) NOT NULL,
gender CHAR(1),
nation_code CHAR(3),
event VARCHAR(30),
CONSTRAINT pk_athlete_code PRIMARY KEY(code)
);

CREATE TABLE code(
s_name CHAR(1),
f_name VARCHAR(6)
);

CREATE TABLE event(
code INT,
sports VARCHAR(50),
name VARCHAR(50),
gender CHAR(1),
players INT,
CONSTRAINT pk_event_code PRIMARY KEY(code)
);

CREATE TABLE game(
host_year INT NOT NULL,
event_code INT NOT NULL,
athlete_code INT NOT NULL,
stadium_code INT NOT NULL,
nation_code CHAR(3),
medal CHAR(1),
game_date DATE,
CONSTRAINT pk_game_host_year_event_code_athlete_code PRIMARY KEY(host_year,event_code,athlete_code)
);

CREATE TABLE history(
event_code INT NOT NULL,
athlete VARCHAR(40) NOT NULL,
host_year INT,
score VARCHAR(10),
unit VARCHAR(5),
CONSTRAINT pk_history_event_code_athlete PRIMARY KEY(event_code,athlete)
);

CREATE TABLE nation(
code CHAR(3),
name VARCHAR(40) NOT NULL,
continent VARCHAR(10),
capital VARCHAR(30),
CONSTRAINT pk_nation_code PRIMARY KEY(code)
);

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
);

CREATE TABLE participant(
host_year INT NOT NULL,
nation_code CHAR(3) NOT NULL,
gold INT DEFAULT 0,
silver INT DEFAULT 0,
bronze INT DEFAULT 0,
CONSTRAINT pk_participant_host_year_nation_code PRIMARY KEY(host_year,nation_code)
);

CREATE TABLE record(
host_year INT NOT NULL,
event_code INT NOT NULL,
athlete_code INT NOT NULL,
medal CHAR(1) NOT NULL,
score VARCHAR(20),
unit VARCHAR(5),
CONSTRAINT pk_record_host_year_event_code_athlete_code_medal PRIMARY KEY(host_year,event_code,athlete_code,medal)
);

CREATE TABLE stadium(
code INT,
nation_code CHAR(3) NOT NULL,
name VARCHAR(50) NOT NULL,
AREA NUMERIC(10,2),
seats INT,
address VARCHAR(100),
CONSTRAINT pk_stadium_code PRIMARY KEY(code)
);

ALTER TABLE game ADD CONSTRAINT fk_game_event_code FOREIGN KEY (event_code) REFERENCES event(code) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE game ADD CONSTRAINT fk_game_athlete_code FOREIGN KEY (athlete_code) REFERENCES athlete(code) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE participant ADD CONSTRAINT fk_participant_host_year FOREIGN KEY (host_year) REFERENCES olympic(host_year) ON DELETE RESTRICT ON UPDATE RESTRICT;
ALTER TABLE participant ADD CONSTRAINT fk_participant_nation_code FOREIGN KEY (nation_code) REFERENCES nation(code) ON DELETE RESTRICT ON UPDATE RESTRICT;

CREATE INDEX idx_athlete_name ON athlete("name");
CREATE INDEX idx_game_game_date ON game(game_date DESC);

CREATE TABLE test_binary (
  f1 BLOB,
  f2 bit(255) DEFAULT NULL,
  f3 bit VARYING(60000) DEFAULT NULL
) ;

CREATE TABLE test_number (
  id INT NOT NULL AUTO_INCREMENT(1,1),
  f1 short DEFAULT NULL,
  f2 SMALLINT DEFAULT NULL,
  f3 integer DEFAULT NULL,
  f4 INT DEFAULT NULL,
  f5 BIGINT DEFAULT NULL,
  f6 FLOAT DEFAULT NULL,
  f7 DOUBLE DEFAULT NULL,
  f8 DECIMAL(38,0) DEFAULT NULL,
  f9 DECIMAL(38,2) DEFAULT NULL,
  PRIMARY KEY  (id)
) ;


CREATE TABLE test_string (
  id VARCHAR(128) NOT NULL,
  f1 CHAR(1) DEFAULT NULL,
  f2 CHAR(2) DEFAULT NULL,
  f3 CHAR(255) DEFAULT NULL,
  f4 VARCHAR(1) DEFAULT NULL,
  f5 VARCHAR(2) DEFAULT NULL,
  f6 VARCHAR(4096) DEFAULT NULL,
  f7 clob,
  PRIMARY KEY  (id)
) ;

CREATE TABLE test_time (
  f1 DATE DEFAULT NULL,
  f2 TIME DEFAULT NULL,
  f3 DATETIME DEFAULT NULL,
  f4 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ;

CREATE TABLE "test_set"(
"f1" set_of(integer),
"f2" sequence_of(character varying(4096)),
"f3" multiset_of(numeric(38,2))
);

CREATE OR REPLACE VIEW "game_view"(
 "host_year" INTEGER,
 "event_code" INTEGER,
 "athlete_code" INTEGER,
 "stadium_code" INTEGER,
 "nation_code" CHAR(3),
 "medal" CHAR(1),
 "game_date" DATE)
    AS 
select a.* from game a, event b where a.event_code=b.code;

CREATE OR REPLACE VIEW "participant_view"(
 "host_year" INTEGER,
 "nation_code" CHAR(3),
 "gold" INTEGER,
 "silver" INTEGER,
 "bronze" INTEGER)
    AS 
select a.* from participant a ,nation b where a.nation_code=b.code;

CREATE TABLE "test_range_partition"(
"f1" integer,
"f2" character varying(4096),
CONSTRAINT pk_test_partition_f1 PRIMARY KEY("f1")
);
ALTER TABLE "test_range_partition" PARTITION BY RANGE (f1) (PARTITION r1 VALUES LESS THAN (1000), PARTITION r2 VALUES LESS THAN MAXVALUE);
	
CREATE TABLE "test_list_partition"(
"f1" integer,
"f2" character varying(4096),
CONSTRAINT pk_test_list_partition_f1 PRIMARY KEY("f1")
);
ALTER TABLE "test_list_partition" PARTITION BY LIST (f2) (PARTITION l1 VALUES IN ('test1','test2','test3'), PARTITION l2 VALUES IN ('test4','test5','test6'));

CREATE TABLE "test_hash_partition"(
"f1" integer,
"f2" character varying(4096),
CONSTRAINT pk_test_hash_partition_f1 PRIMARY KEY("f1")
);
ALTER TABLE "test_hash_partition" PARTITION BY HASH (f1) PARTITIONS 2;

CREATE SERIAL test_sequence 
INCREMENT BY 1
START WITH 1
NOMAXVALUE
NOCYCLE
CACHE 10; 

CREATE PROCEDURE "test_proc"("p1" INTEGER)
AS LANGUAGE JAVA 
NAME 'testsp.test_proc(int)'