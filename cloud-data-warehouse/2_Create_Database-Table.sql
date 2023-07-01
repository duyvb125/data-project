CREATE DATABASE ASM3_Source;
GO

-- Tạo các bảng dữ liệu nguồn
CREATE TABLE netflix_shows (
	show_id INT NOT NULL,
	type VARCHAR(255),
	title NVARCHAR(255),
	director NVARCHAR(255),
	cast NVARCHAR(255),
	country VARCHAR(255),
	date_added DATE,
	release_year SMALLINT,
	rating VARCHAR(255),
	duration VARCHAR(255),
	listed_in NVARCHAR(255),
	description NVARCHAR(1000)
	)
GO

-- Tạo các bảng DIM
CREATE TABLE dim_country (
	country_id INT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	country VARCHAR(255))

CREATE TABLE dim_director (
	director_id INT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	director NVARCHAR(255))

CREATE TABLE dim_date (
	date_id INT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	date_added DATE NOT NULL,
	release_year TINYINT NOT NULL)

CREATE TABLE dim_rating (
	rating_id INT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	rating VARCHAR(255))

CREATE TABLE dim_duration (
	duration_id INT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	duration VARCHAR(255))

CREATE TABLE dim_info (
	info_id INT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	title NVARCHAR(255),
	listed_in NVARCHAR(255),
	description NVARCHAR(1000),
	cast NVARCHAR(255))

CREATE TABLE dim_type (
	type_id INT NOT NULL PRIMARY KEY CLUSTERED IDENTITY(1,1),
	type VARCHAR(255))
GO

-- Tạo bảng FACT
CREATE TABLE fact_netflix_shows (
	show_id INT NOT NULL,
	info_id INT NOT NULL FOREIGN KEY REFERENCES dim_info(info_id),
	type_id INT NOT NULL FOREIGN KEY REFERENCES dim_type(type_id),
	director_id INT NOT NULL FOREIGN KEY REFERENCES dim_director(director_id),
	country_id INT NOT NULL FOREIGN KEY REFERENCES dim_country(country_id),
	date_id INT NOT NULL FOREIGN KEY REFERENCES dim_date(date_id),
	rating_id INT NOT NULL FOREIGN KEY REFERENCES dim_rating(rating_id),
	duration_id INT NOT NULL FOREIGN KEY REFERENCES dim_duration(duration_id),
	)
GO

-- Tạo Procedure load vào DIM
CREATE PROCEDURE load_dim_type
AS
INSERT INTO dim_type (type)
	SELECT DISTINCT source.type
	FROM DEP304_ASM3_Source.dbo.netflix_shows AS source
	WHERE NOT EXISTS (
		SELECT 1 
		FROM dim_type AS dim
		WHERE dim.type = source.type
		);
GO

CREATE PROCEDURE load_dim_country
AS
INSERT INTO dim_country (country)
	SELECT DISTINCT source.country
	FROM DEP304_ASM3_Source.dbo.netflix_shows AS source
	WHERE NOT EXISTS (
		SELECT 1 
		FROM dim_country AS dim
		WHERE dim.country = source.country
		);
GO

CREATE PROCEDURE load_dim_director
AS
INSERT INTO dim_director (director)
	SELECT DISTINCT source.director
	FROM DEP304_ASM3_Source.dbo.netflix_shows AS source
	WHERE NOT EXISTS (
		SELECT 1 
		FROM dim_director AS dim
		WHERE dim.director = source.director
		);
GO

CREATE PROCEDURE load_dim_date
AS
INSERT INTO dim_date (date_added, release_year)
	SELECT DISTINCT source.date_added, source.release_year
	FROM DEP304_ASM3_Source.dbo.netflix_shows AS source
	WHERE NOT EXISTS (
		SELECT 1 
		FROM dim_date AS dim
		WHERE 
			dim.date_added IS NULL AND dim.release_year = source.release_year
			OR
			dim.date_added = source.date_added AND dim.release_year = source.release_year
		);
GO

CREATE PROCEDURE load_dim_rating
AS
INSERT INTO dim_rating (rating)
	SELECT DISTINCT source.rating
	FROM DEP304_ASM3_Source.dbo.netflix_shows AS source
	WHERE NOT EXISTS (
		SELECT 1 
		FROM dim_rating AS dim
		WHERE dim.rating = source.rating
		);
GO

CREATE PROCEDURE load_dim_duration
AS
INSERT INTO dim_duration (duration)
	SELECT DISTINCT source.duration
	FROM DEP304_ASM3_Source.dbo.netflix_shows AS source
	WHERE NOT EXISTS (
		SELECT 1 
		FROM dim_duration AS dim
		WHERE dim.duration = source.duration
		);
GO

CREATE PROCEDURE load_dim_info
AS
INSERT INTO dim_info (title, listed_in, description, cast)
	SELECT DISTINCT source.title, source.listed_in, source.description, source.cast
	FROM DEP304_ASM3_Source.dbo.netflix_shows AS source
	WHERE NOT EXISTS (
		SELECT 1 
		FROM dim_info AS dim
		WHERE dim.title = source.title
		);
GO

CREATE PROCEDURE load_fact_netflix_shows
AS
	INSERT INTO fact_netflix_shows (show_id, info_id, type_id, director_id, country_id, date_id, rating_id, duration_id)
	SELECT show_id, info_id, type_id, director_id, country_id, date_id, rating_id, duration_id
	FROM DEP304_ASM3_Source.dbo.netflix_shows AS source
	LEFT JOIN dim_info
		ON dim_info.cast = source.cast
		AND dim_info.description = source.description
		AND dim_info.listed_in = source.listed_in
		AND dim_info.title = source.title
	LEFT JOIN dim_type ON dim_type.type = source.type
	LEFT JOIN dim_director ON dim_director.director = source.director
	LEFT JOIN dim_country ON dim_country.country = source.country
	LEFT JOIN dim_date
		ON dim_date.date_added = source.date_added
		AND dim_date.release_year = source.release_year
	LEFT JOIN dim_rating ON dim_rating.rating = source.rating
	LEFT JOIN dim_duration ON dim_duration.duration = source.duration
GO