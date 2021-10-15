CREATE TABLE "athlete"(
"code" integer AUTO_INCREMENT (2,1),
"name" character varying(40) NOT NULL,
"gender" character(1),
"nation_code" character(3),
"event" character varying(30)
)
;
CREATE TABLE "code"(
"s_name" character(1),
"f_name" character varying(6)
)
;
CREATE TABLE "event"(
"code" integer,
"sports" character varying(50),
"name" character varying(50),
"gender" character(1),
"players" integer
)
;
CREATE TABLE "game"(
"host_year" integer,
"event_code" integer,
"athlete_code" integer,
"stadium_code" integer NOT NULL,
"nation_code" character(3),
"medal" character(1),
"game_date" date
)
;
CREATE TABLE "history"(
"event_code" integer,
"athlete" character varying(40),
"host_year" integer,
"score" character varying(10),
"unit" character varying(5)
)
;
CREATE TABLE "nation"(
"code" character(3),
"name" character varying(40) NOT NULL,
"continent" character varying(10),
"capital" character varying(30)
)
;
CREATE TABLE "olympic"(
"host_year" integer,
"host_nation" character varying(40) NOT NULL,
"host_city" character varying(20) NOT NULL,
"opening_date" date NOT NULL,
"closing_date" date NOT NULL,
"mascot" character varying(20),
"slogan" character varying(40),
"introduction" character varying(1500)
)
;
CREATE TABLE "participant"(
"host_year" integer,
"nation_code" character(3),
"gold" integer DEFAULT 0,
"silver" integer DEFAULT 0,
"bronze" integer DEFAULT 0
)
;
CREATE TABLE "record"(
"host_year" integer,
"event_code" integer,
"athlete_code" integer,
"medal" character(1),
"score" character varying(20),
"unit" character varying(5)
)
;
CREATE TABLE "stadium"(
"code" integer,
"nation_code" character(3) NOT NULL,
"name" character varying(50) NOT NULL,
"area" numeric(10,2),
"seats" integer,
"address" character varying(100)
)
;
CREATE TABLE "test_binary"(
"f1" blob,
"f2" bit(255),
"f3" bit varying(60000)
)
;
CREATE TABLE "test_hash_partition"(
"f1" integer,
"f2" character varying(4096)
)
PARTITION BY HASH ("f1") 
PARTITIONS 2
;
CREATE TABLE "test_list_partition"(
"f1" integer,
"f2" character varying(4096)
)
PARTITION BY LIST ("f2") ( 
PARTITION l1 VALUES IN (test1,test2,test3),
PARTITION l2 VALUES IN (test4,test5,test6)
 ) 
;
CREATE TABLE "test_number"(
"id" integer AUTO_INCREMENT (2,1),
"f1" smallint,
"f2" smallint,
"f3" integer,
"f4" integer,
"f5" bigint,
"f6" float,
"f7" double,
"f8" numeric(38,0),
"f9" numeric(38,2)
)
;
CREATE TABLE "test_range_partition"(
"f1" integer,
"f2" character varying(4096)
)
PARTITION BY RANGE ("f1") ( 
PARTITION r1 VALUES LESS THAN (1000),
PARTITION r2 VALUES LESS THAN MAXVALUE
 ) 
;
CREATE TABLE "test_set"(
"f1" set_of(integer),
"f2" sequence_of(character varying(4096)),
"f3" multiset_of(numeric(38,0))
)
;
CREATE TABLE "test_string"(
"id" character varying(128),
"f1" character(1),
"f2" character(2),
"f3" character(255),
"f4" character varying(1),
"f5" character varying(2),
"f6" character varying(4096),
"f7" clob
)
;
CREATE TABLE "test_time"(
"f1" date,
"f2" time,
"f3" datetime,
"f4" timestamp DEFAULT TIMESTAMP'02/05/2013 16:16:29' NOT NULL
)
;
CREATE OR REPLACE VIEW "game_view"    AS 
select [a].[host_year], [a].[event_code], [a].[athlete_code], [a].[stadium_code], [a].[nation_code], [a].[medal], [a].[game_date] from [game] [a], [event] [b] where ([a].[event_code]=[b].[code]);
CREATE OR REPLACE VIEW "participant_view"    AS 
select [a].[host_year], [a].[nation_code], [a].[gold], [a].[silver], [a].[bronze] from [participant] [a], [nation] [b] where ([a].[nation_code]=[b].[code]);
CREATE SERIAL "test_sequence" START WITH 1 INCREMENT BY 1 NOMINVALUE  NOMAXVALUE  NOCYCLE CACHE 10;
