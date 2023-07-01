-- Tạo procedure load dữ liệu vào DIM

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
		WHERE 
			dim.director = source.director
			OR
			dim.director IS NULL
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
		WHERE 
			dim.rating = source.rating
			OR
			dim.rating IS NULL
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
		WHERE 
			dim.duration = source.duration
			OR
			dim.duration IS NULL
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

-- Tạo procedure load dữ liệu vào FACT

ALTER PROCEDURE load_fact_netflix_shows
AS
	INSERT INTO fact_netflix_shows (show_id, info_id, type_id, director_id, country_id, date_id, rating_id, duration_id)
	SELECT show_id, info_id, type_id, ISNULL(director_id, 1), ISNULL(country_id, 1), ISNULL(date_id, 1), ISNULL(rating_id, 1), ISNULL(duration_id, 1)
	FROM DEP304_ASM3_Source.dbo.netflix_shows AS source
	LEFT JOIN dim_info ON dim_info.title = source.title
	LEFT JOIN dim_type ON dim_type.type = source.type
	LEFT JOIN dim_director ON dim_director.director = source.director
	LEFT JOIN dim_country ON dim_country.country = source.country
	LEFT JOIN dim_date
		ON dim_date.date_added = source.date_added
		AND dim_date.release_year = source.release_year
	LEFT JOIN dim_rating ON dim_rating.rating = source.rating
	LEFT JOIN dim_duration ON dim_duration.duration = source.duration
GO

-- Chạy procedure
EXEC load_dim_type
EXEC load_dim_date
EXEC load_dim_director
EXEC load_dim_duration
EXEC load_dim_info
EXEC load_dim_rating
EXEC load_dim_type
EXEC load_fact_netflix_shows