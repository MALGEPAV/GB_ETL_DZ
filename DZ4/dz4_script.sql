DROP RULE IF EXISTS movies_insert_pre_1990 ON movies;	
DROP RULE IF EXISTS movies_insert_between_1990_and_2000 ON movies;
DROP RULE IF EXISTS movies_insert_between_2000_and_2010 ON movies;
DROP RULE IF EXISTS movies_insert_between_2010_and_2020 ON movies; 
DROP RULE IF EXISTS movies_insert_after_2020 ON movies;
DROP RULE IF EXISTS movies_insert_under_40min ON movies; 
DROP RULE IF EXISTS movies_insert_between_40_and_90min ON movies;
DROP RULE IF EXISTS movies_insert_between_90_and_130min ON movies;
DROP RULE IF EXISTS movies_insert_over_130min ON movies;
DROP RULE IF EXISTS movies_insert_bad_movies ON movies;
DROP RULE IF EXISTS movies_insert_good_movies ON movies;
DROP RULE IF EXISTS movies_insert_excellent_movies ON movies;
DROP RULE IF EXISTS movies_excellent_movies ON movies;

DROP TABLE IF EXISTS movies_pre_1990;
DROP TABLE IF EXISTS movies_between_1990_and_2000;
DROP TABLE IF EXISTS movies_between_2000_and_2010;
DROP TABLE IF EXISTS movies_between_2010_and_2020;
DROP TABLE IF EXISTS movies_after_2020;

DROP TABLE IF EXISTS movies_under_40min;
DROP TABLE IF EXISTS movies_between_40_and_90min;
DROP TABLE IF EXISTS movies_between_90_and_130min;
DROP TABLE IF EXISTS movies_over_130min;

DROP TABLE IF EXISTS bad_movies;
DROP TABLE IF EXISTS good_movies;
DROP TABLE IF EXISTS excellent_movies;

DROP TABLE IF EXISTS movies;
CREATE TABLE IF NOT EXISTS movies(
	title VARCHAR(45),
	movies_type VARCHAR(45),
	director VARCHAR(45),
	year_of_issue INT,
	length_in_minutes INT,
	rate INT
);
-- Таблицы для партицирования по году выпуска

CREATE TABLE IF NOT EXISTS movies_pre_1990 
	( CHECK (year_of_issue < 1990)) INHERITS (movies);


CREATE TABLE IF NOT EXISTS movies_between_1990_and_2000 
	( CHECK (year_of_issue >= 1990 AND year_of_issue < 2000)) INHERITS (movies);


CREATE TABLE IF NOT EXISTS movies_between_2000_and_2010 
	( CHECK (year_of_issue >= 2000 AND year_of_issue < 2010)) INHERITS (movies);	


CREATE TABLE IF NOT EXISTS movies_between_2010_and_2020 
	( CHECK (year_of_issue >= 2010 AND year_of_issue < 2020)) INHERITS (movies);


CREATE TABLE IF NOT EXISTS movies_after_2020 
	( CHECK (year_of_issue >= 2020 )) INHERITS (movies);

-- Таблицы для партицирования по длине фильма

CREATE TABLE IF NOT EXISTS movies_under_40min 
	( CHECK (length_in_minutes < 40)) INHERITS (movies);


CREATE TABLE IF NOT EXISTS movies_between_40_and_90min 
	( CHECK (length_in_minutes >= 40 AND length_in_minutes < 90)) INHERITS (movies);


CREATE TABLE IF NOT EXISTS movies_between_90_and_130min 
	( CHECK (length_in_minutes >= 90 AND length_in_minutes < 130)) INHERITS (movies);


CREATE TABLE IF NOT EXISTS movies_over_130min 
	( CHECK (length_in_minutes >= 130)) INHERITS (movies);

-- Таблицы для партицирования по рейтингу


CREATE TABLE IF NOT EXISTS bad_movies 
	( CHECK (rate < 5)) INHERITS (movies);


CREATE TABLE IF NOT EXISTS good_movies 
	( CHECK (rate >= 5 AND rate < 8)) INHERITS (movies);


CREATE TABLE IF NOT EXISTS excellent_movies 
	( CHECK (rate >= 8)) INHERITS (movies);

-- Правила

CREATE RULE  movies_insert_pre_1990 AS ON 
	INSERT TO movies WHERE (year_of_issue < 1990) DO INSTEAD 
	INSERT INTO movies_pre_1990 VALUES (NEW.*);

CREATE RULE  movies_insert_between_1990_and_2000 AS ON 
	INSERT TO movies WHERE (year_of_issue >= 1990 AND year_of_issue < 2000) DO INSTEAD 
	INSERT INTO movies_between_1990_and_2000 VALUES (NEW.*);

CREATE RULE  movies_insert_between_2000_and_2010 AS ON 
	INSERT TO movies WHERE (year_of_issue >= 2000 AND year_of_issue < 2010) DO INSTEAD 
	INSERT INTO movies_between_2000_and_2010 VALUES (NEW.*);

CREATE RULE  movies_insert_between_2010_and_2020 AS ON 
	INSERT TO movies WHERE (year_of_issue >= 2010 AND year_of_issue < 2020) DO INSTEAD 
	INSERT INTO movies_between_2010_and_2020 VALUES (NEW.*);

CREATE RULE  movies_insert_after_2020 AS ON 
	INSERT TO movies WHERE (year_of_issue >= 2020) DO INSTEAD 
	INSERT INTO movies_after_2020 VALUES (NEW.*);

CREATE RULE  movies_insert_under_40min AS ON 
	INSERT TO movies WHERE (length_in_minutes < 40) DO INSTEAD 
	INSERT INTO movies_under_40min VALUES (NEW.*);

CREATE RULE  movies_insert_between_40_and_90min AS ON 
	INSERT TO movies WHERE (length_in_minutes >= 40 AND length_in_minutes < 90) DO INSTEAD 
	INSERT INTO movies_between_40_and_90min VALUES (NEW.*);

CREATE RULE  movies_insert_between_90_and_130min AS ON 
	INSERT TO movies WHERE (length_in_minutes >= 90 AND length_in_minutes < 130) DO INSTEAD 
	INSERT INTO movies_between_90_and_130min VALUES (NEW.*);

CREATE RULE  movies_insert_over_130min AS ON 
	INSERT TO movies WHERE (length_in_minutes >= 130) DO INSTEAD 
	INSERT INTO movies_over_130min VALUES (NEW.*);

CREATE RULE  movies_insert_bad_movies AS ON 
	INSERT TO movies WHERE (rate < 5) DO INSTEAD 
	INSERT INTO bad_movies VALUES (NEW.*);

CREATE RULE  movies_insert_good_movies AS ON 
	INSERT TO movies WHERE (rate >= 5 AND rate < 8) DO INSTEAD 
	INSERT INTO good_movies VALUES (NEW.*);

CREATE RULE  movies_insert_excellent_movies AS ON 
	INSERT TO movies WHERE (rate >= 8) DO INSTEAD 
	INSERT INTO excellent_movies VALUES (NEW.*);

INSERT INTO movies(title, movies_type, director, year_of_issue, length_in_minutes, rate)
VALUES
('Иван Васильевич меняет профессию','комедия','Гайдай', 1973, 88, 9),
('Служебный роман','комедия','Рязанов',1977,159,8),
('Джентельмены удачи','комедия','Серый',1971,84,9),
('Брат 3','криминал','Переверзев',2023,117,4),
('Свидетель','драма','Дадунашвили',2023,128,3),
('Наша Russia: Яйца судьбы','комедия','Орлов',2010,86,4),
('Алиса и Вирил','драма','Шейнберг',1993,2,5),
('Кешка и Фреди','комедия','Берзнер',1992,18,4),
('Анджелла и Вирил','драма','Шейнберг',1993,2,5),
('Помутнение','фантастика','Линклейтер',2006,100,7),
('Властелин колец: Возвращение короля','фэнтези','Джексон',2003,201,9),
('Властелин колец: Братство Кольца','фэнтези','Джексон',2001,178,9),
('Интерстеллар','фантастика','Нолан',2014,169,9),
('Джентльмены','криминал','Ричи',2019,113,9),
('Оппенгеймер','биография','Нолан',2023,180,8),
('Самолет летит в Россию','комедия','Капилевич',1994,94,100500),
('Самолет летит в Россию','комедия','Капилевич',1994,94,100500), -- рейтинг выше 10
('У Фантоцци опять неприятности','комедия','Паренти',1983,84,100500); -- рейтинг выше 10

-- select из всех таблиц
SELECT *  FROM movies;

-- select только из основной таблицы
SELECT *  FROM ONLY movies;

-- select из других таблиц
SELECT *  FROM  movies_pre_1990;
SELECT *  FROM  movies_between_90_and_130min;
SELECT *  FROM  excellent_movies;
