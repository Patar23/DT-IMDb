-- Nastavenie databázy a schémy
USE DATABASE FLAMINGO_IMDB;
USE SCHEMA imdb_schema;

-- Vytvorenie stage pre načítanie dát
CREATE OR REPLACE STAGE FLAMINGO_STAGE;

-- Vytvorenie staging tabuľky movie
CREATE OR REPLACE TABLE movie_staging (
	ID VARCHAR(10) NOT NULL,
	TITLE VARCHAR(200),
	YEAR INT,
	DATE_PUBLISHED DATE,
	DURATION INT,
	COUNTRY VARCHAR(250),
	WORLWIDE_GROSS_INCOME VARCHAR(30),
	LANGUAGES VARCHAR(200),
	PRODUCTION_COMPANY VARCHAR(200),
	primary key (ID)
);

-- Vytvorenie staging tabuľky names
CREATE OR REPLACE TABLE names_staging (
	ID VARCHAR(10) NOT NULL,
	NAME VARCHAR(100),
	HEIGHT INT,
	DATE_OF_BIRTH DATE,
	KNOWN_FOR_MOVIES VARCHAR(100),
	primary key (ID)
);

-- Vytvorenie staging tabuľky ratings
CREATE OR REPLACE TABLE ratings_staging (
	MOVIE_ID VARCHAR(10) NOT NULL,
	AVG_RATING DECIMAL(3,1),
	TOTAL_VOTES INT,
	MEDIAN_RATING INT,
	primary key (MOVIE_ID),
	foreign key (MOVIE_ID) references movie_staging(ID)
);

-- Vytvorenie staging tabuľky genre
CREATE OR REPLACE TABLE genre_staging (
	MOVIE_ID VARCHAR(10) NOT NULL,
	GENRE VARCHAR(20) NOT NULL,
	primary key (MOVIE_ID, GENRE),
	foreign key (MOVIE_ID) references movie_staging(ID)
);

-- Vytvorenie staging tabuľky director_mapping
CREATE OR REPLACE TABLE director_mapping_staging (
	MOVIE_ID VARCHAR(10) NOT NULL,
	NAME_ID VARCHAR(10) NOT NULL,
	primary key (MOVIE_ID, NAME_ID),
	foreign key (MOVIE_ID) references movie_staging(ID),
	foreign key (NAME_ID) references names_staging(ID)
);

-- Vytvorenie staging tabuľky role_mapping
CREATE OR REPLACE TABLE role_mapping_staging (
	MOVIE_ID VARCHAR(10) NOT NULL,
	NAME_ID VARCHAR(10) NOT NULL,
	CATEGORY VARCHAR(10),
	primary key (MOVIE_ID, NAME_ID),
	foreign key (MOVIE_ID) references movie_staging(ID),
	foreign key (NAME_ID) references names_staging(ID)
);

-- Načítanie dát do staging tabuliek z csv súborov

COPY INTO movie_staging
FROM @FLAMINGO_STAGE/movie.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO names_staging
FROM @FLAMINGO_STAGE/names.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO ratings_staging
FROM @FLAMINGO_STAGE/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO genre_staging
FROM @FLAMINGO_STAGE/genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';


COPY INTO director_mapping_staging
FROM @FLAMINGO_STAGE/director_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO role_mapping_staging
FROM @FLAMINGO_STAGE/role_mapping.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';


--- Transformácia dát

-- Vytvorenie dimenzionálnej tabuľky pre movies
CREATE TABLE dim_movies AS
SELECT DISTINCT
    id AS dim_movieId,         
    title,                           
    year,                           
    TO_DATE(date_published) AS date_published,
    duration,                      
    country,                       
    languages,                    
    production_company         
FROM movie_staging;

-- Vytvorenie dimenzionálnej tabuľky pre genres
CREATE TABLE dim_genres AS
SELECT DISTINCT
    movie_id AS dim_genreId,        
    genre AS genre_name              
FROM genre_staging;

-- Vytvorenie dimenzionálnej tabuľky pre names
CREATE TABLE dim_names AS
SELECT DISTINCT
    id AS dim_nameId,            
    name,                           
    height,                          
    TO_DATE(date_of_birth) AS date_of_birth,
    known_for_movies          
FROM names_staging;

-- Vytvorenie dimenzionálnej tabuľky pre directors
CREATE TABLE dim_directors AS
SELECT DISTINCT
    dm.name_id AS director_id,     
    n.name AS name               
FROM director_mapping_staging dm
JOIN names_staging n
  ON dm.name_id = n.id; 

  -- Vytvorenie dimenzionálnej tabuľky pre roles
CREATE TABLE dim_roles AS
SELECT DISTINCT
    rm.name_id AS role_id,        
    n.name AS role_name,          
    category
FROM role_mapping_staging rm
JOIN names_staging n
  ON rm.name_id = n.id;      

-- Vytvorenie tabuľky fact_movies
CREATE TABLE fact_movies AS
SELECT 
    m.id AS fact_movie_id,                 -- Unikátne ID filmu
    r.avg_rating,                          -- Priemerné hodnotenie filmu
    r.total_votes,                         -- Počet hlasov pre film
    m.worlwide_gross_income,               -- Celkový príjem filmu na celom svete
    g.movie_id AS genre_id,                -- Prepojenie s dimenziou žánrov
    d.name_id AS director_id,              -- Prepojenie s dimenziou režisérov
    m.id AS movie_id,                      -- Prepojenie s dimenziou filmov
    n.id AS name_id,                       -- Prepojenie s dimenziou mien hercov
    ro.name_id AS role_id                  -- Prepojenie s dimenziou rolí hercov
FROM ratings_staging r
JOIN movie_staging m ON r.movie_id = m.id                      -- Prepojenie s tabuľkou filmov
LEFT JOIN genre_staging g ON m.id = g.movie_id                 -- Prepojenie s tabuľkou žánrov
LEFT JOIN director_mapping_staging d ON m.id = d.movie_id      -- Prepojenie s tabuľkou režisérov
LEFT JOIN role_mapping_staging ro ON m.id = ro.movie_id        -- Prepojenie s tabuľkou rolí
LEFT JOIN names_staging n ON ro.name_id = n.id;                -- Prepojenie s tabuľkou mien


-- Odstránenie staging tabuliek
DROP TABLE IF EXISTS movie_staging;
DROP TABLE IF EXISTS names_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS genre_staging;
DROP TABLE IF EXISTS director_mapping_staging;
DROP TABLE IF EXISTS role_mapping_staging;