CREATE TABLE athlete(
code INT identity(1,1),
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
area NUMERIC(10,2),
seats INT,
address VARCHAR(100),
CONSTRAINT pk_stadium_code PRIMARY KEY(code)
);


ALTER TABLE game ADD CONSTRAINT fk_game_event_code FOREIGN KEY (event_code) REFERENCES event(code) ;
ALTER TABLE game ADD CONSTRAINT fk_game_athlete_code FOREIGN KEY (athlete_code) REFERENCES athlete(code);
ALTER TABLE participant ADD CONSTRAINT fk_participant_host_year FOREIGN KEY (host_year) REFERENCES olympic(host_year);
ALTER TABLE participant ADD CONSTRAINT fk_participant_nation_code FOREIGN KEY (nation_code) REFERENCES nation(code) ;

CREATE INDEX idx_athlete_name ON athlete(name);
CREATE INDEX idx_game_game_date ON game(game_date DESC);

CREATE TABLE test_binary (
  f1 image,
  f2 binary(8000) DEFAULT NULL,
  f3 VARbinary(8000) DEFAULT NULL,
  f4 bit DEFAULT NULL
) ;

CREATE TABLE test_number (
  id INT NOT NULL identity(1,1),
  f1 TINYINT DEFAULT NULL,
  f2 SMALLINT DEFAULT NULL,
  f3 INT DEFAULT NULL,
  f4 BIGINT DEFAULT NULL,
  f5 FLOAT DEFAULT NULL,
  f6 money DEFAULT NULL,
  f7 smallmoney DEFAULT NULL,
  f8 DECIMAL DEFAULT NULL,
  f9 DECIMAL(38,2) DEFAULT NULL,
  f10 numeric DEFAULT NULL,
  f11 real DEFAULT NULL,
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
  f7 text,
  f8 xml
  PRIMARY KEY  (id)
) ;

CREATE TABLE test_time (
  f1 smallDATEtime DEFAULT NULL,
  f2 TIME DEFAULT NULL,
  f3 DATETIME DEFAULT NULL
) ;

CREATE VIEW "game_view" as
select a.* from game a, event b where a.event_code=b.code;

CREATE VIEW participant_view
    AS 
select a.* from participant a ,nation b where a.nation_code=b.code;