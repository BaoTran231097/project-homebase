
-- SCRIPT CREATE TABLE --
CREATE OR REPLACE TABLE clickhousedb.wine_data
(
    classes String,
    alcohol Float32,
    malic_acid Float32,
    ash Float32,
    alcalinity_of_ash Float32,
    magnesium Int32,
    total_phenols Float32,
    flavanoids Float32,
    nonflavanoid_phenols Float32,
    proanthocyanins Float32,
    color_intensity Float32,
    hue Float32,
    proline Float32,
    quality Int32,
    process_date DateTime64
)
ENGINE = MergeTree()
ORDER BY classes;

-- SCRIPT GET DATA IN TABLE --
SELECT * FROM clickhousedb.wine_data;

-- TRUNCATE TABLE --
truncate table clickhousedb.wine_data;


-- Write ClickHouse SQL queries to answer: 1. How many unique values are in variable X?
SELECT uniqExact(alcohol) AS unique_count
FROM clickhousedb.wine_data
LIMIT 1;


-- Write ClickHouse SQL queries to answer: 2. What is the average of variable Y grouped by variable Z?
SELECT classes, AVG(alcalinity_of_ash) AS avg_of_alcalinity_of_ash
FROM clickhousedb.wine_data
GROUP BY classes