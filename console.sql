-- Raw data loading

SELECT * FROM READ_JSON_AUTO('/Users/iv/Downloads/steam_2025_5k-dataset-games_20250831.json', maximum_object_size = 1073741824);

CREATE OR REPLACE TABLE raw_applications_games AS
    SELECT UNNEST(games) AS g
    FROM READ_JSON_AUTO('/Users/iv/Downloads/steam_2025_5k-dataset-games_20250831.json', maximum_object_size = 1073741824);

SELECT * FROM raw_applications_games;


SELECT * FROM READ_JSON_AUTO('/Users/iv/Downloads/steam_2025_5k-dataset-reviews_20250901.json', maximum_object_size = 1073741824);

CREATE OR REPLACE TABLE raw_reviews AS
    SELECT UNNEST(reviews) AS u
    FROM READ_JSON_AUTO('/Users/iv/Downloads/steam_2025_5k-dataset-reviews_20250901.json', maximum_object_size = 1073741824);

SELECT * FROM raw_reviews;

CREATE OR REPLACE TABLE exchange_rates (
    currency_code VARCHAR(3) PRIMARY KEY,
    currency_name VARCHAR(50),
    rate_to_usd DECIMAL(18, 10), -- Stores the value of 1 Foreign Unit in USD
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO exchange_rates (currency_name, currency_code, rate_to_usd) VALUES
('Argentine Peso', 'ARS', 0.000691),
('Australian Dollar', 'AUD', 0.649478),
('Bahraini Dinar', 'BHD', 2.659574),
('Botswana Pula', 'BWP', 0.072989),
('Brazilian Real', 'BRL', 0.185875),
('British Pound', 'GBP', 1.319844),
('Bruneian Dollar', 'BND', 0.770191),
('Bulgarian Lev', 'BGN', 0.591256),
('Canadian Dollar', 'CAD', 0.709614),
('Chilean Peso', 'CLP', 0.001073),
('Chinese Yuan Renminbi', 'CNY', 0.141283),
('Colombian Peso', 'COP', 0.000263),
('Czech Koruna', 'CZK', 0.047839),
('Danish Krone', 'DKK', 0.154828),
('Emirati Dirham', 'AED', 0.272294),
('Euro', 'EUR', 1.156395),
('Hong Kong Dollar', 'HKD', 0.128570),
('Hungarian Forint', 'HUF', 0.003026),
('Icelandic Krona', 'ISK', 0.007877),
('Indian Rupee', 'INR', 0.011208),
('Indonesian Rupiah', 'IDR', 0.000060),
('Iranian Rial', 'IRR', 0.000024),
('Israeli Shekel', 'ILS', 0.304770),
('Japanese Yen', 'JPY', 0.006385),
('Kazakhstani Tenge', 'KZT', 0.001928),
('Kuwaiti Dinar', 'KWD', 3.256249),
('Libyan Dinar', 'LYD', 0.183135),
('Malaysian Ringgit', 'MYR', 0.241780),
('Mauritian Rupee', 'MUR', 0.021655),
('Mexican Peso', 'MXN', 0.054407),
('Nepalese Rupee', 'NPR', 0.007002),
('New Zealand Dollar', 'NZD', 0.567341),
('Norwegian Krone', 'NOK', 0.097670),
('Omani Rial', 'OMR', 2.597913),
('Pakistani Rupee', 'PKR', 0.003540),
('Philippine Peso', 'PHP', 0.016993),
('Polish Zloty', 'PLN', 0.273255),
('Qatari Riyal', 'QAR', 0.274725),
('Romanian New Leu', 'RON', 0.227154),
('Russian Ruble', 'RUB', 0.012737),
('Saudi Arabian Riyal', 'SAR', 0.266667),
('Singapore Dollar', 'SGD', 0.770191),
('South African Rand', 'ZAR', 0.058311),
('South Korean Won', 'KRW', 0.000678),
('Sri Lankan Rupee', 'LKR', 0.003248),
('Swedish Krona', 'SEK', 0.104966),
('Swiss Franc', 'CHF', 1.238390),
('Taiwan New Dollar', 'TWD', 0.031904),
('Thai Baht', 'THB', 0.031013),
('Trinidadian Dollar', 'TTD', 0.147395),
('Turkish Lira', 'TRY', 0.023562),
('United States Dollar', 'USD', 1);

SELECT * FROM exchange_rates;


-- Cleaned tables

CREATE OR REPLACE TABLE steam_applications AS SELECT
    g.appid AS app_id,
    g.name_from_applist AS game_name,
    g.app_details.data.price_overview.initial AS initial_price,
    g.app_details.data.price_overview.final AS final_price,
    g.app_details.data.price_overview.currency,
    g.app_details.data.price_overview.discount_percent,
    g.app_details.data.price_overview.final_formatted,
    ROUND(g.app_details.data.price_overview.initial*e.rate_to_usd, 2) AS initial_price_usd,
    ROUND(g.app_details.data.price_overview.final*e.rate_to_usd, 2) AS final_price_usd,
    g.app_details.success AS fetch_success,
    g.app_details.fetched_at,
    g.app_details.data.type,
    g.app_details.data.is_free,
    g.app_details.data.required_age::INTEGER AS required_age,
    g.app_details.data.website,
    g.app_details.data.developers,
    g.app_details.data.platforms.windows,
    g.app_details.data.platforms.mac,
    g.app_details.data.platforms.linux,
    g.app_details.data.categories,
    NOT(g.app_details.data.release_date.coming_soon) AS is_out,
    g.app_details.data.release_date.date AS release_date,
    g.app_details.data.genres

FROM raw_applications_games
LEFT JOIN exchange_rates AS e ON e.currency_code = g.app_details.data.price_overview.currency;

SELECT * FROM steam_applications;

CREATE OR REPLACE TABLE applications_developers AS SELECT
    steam_applications.app_id,
    steam_applications.game_name,
    UNNEST(steam_applications.developers) AS game_devs
FROM steam_applications;

SELECT * FROM applications_developers;

CREATE OR REPLACE TABLE applications_genres AS SELECT
    steam_applications.app_id,
    steam_applications.game_name,
    UNNEST(steam_applications.genres).id AS description_id,
    UNNEST(steam_applications.genres).description AS description_name
FROM steam_applications;

SELECT * FROM applications_genres;

CREATE OR REPLACE TABLE reviews AS SELECT
    u.appid AS app_id,
    u.review_data.query_summary.num_reviews,
    u.review_data.query_summary.review_score,
    u.review_data.query_summary.review_score_desc,
    u.review_data.query_summary.total_positive,
    u.review_data.query_summary.total_negative,
    u.review_data.query_summary.total_reviews,
    u.review_data.reviews
FROM raw_reviews;

SELECT * FROM reviews;

CREATE OR REPLACE TABLE reviews_comments AS SELECT
    app_id,
    UNNEST(reviews.reviews).recommendationid,
    UNNEST(reviews.reviews).author.steamid AS author_id,
    UNNEST(reviews.reviews).author.num_games_owned AS num_games_owned,
    UNNEST(reviews.reviews).author.num_reviews AS num_reviews,
    UNNEST(reviews.reviews).author.playtime_forever AS time_this_game,
    UNNEST(reviews.reviews).author.playtime_at_review AS time_by_review,
    UNNEST(reviews.reviews).language AS review_language,
    UNNEST(reviews.reviews).review AS review_text,
    UNNEST(reviews.reviews).comment_count AS num_comments,
    UNNEST(reviews.reviews).written_during_early_access AS early_acces_comment
FROM reviews;

SELECT * FROM reviews_comments
WHERE review_language = 'ukrainian';


-- Analytical insights

-- 1. Top 10 developers by number of games they made
SELECT game_devs, COUNT(DISTINCT(app_id)) AS num_games FROM applications_developers
GROUP BY game_devs
ORDER BY num_games DESC
LIMIT 10;

-- 2. Top 3 the most expensive games from each developer
SELECT d.game_devs, s.game_name, ROUND(s.initial_price_usd/100, 2) AS price_usd, ROW_NUMBER() OVER (PARTITION BY d.game_devs ORDER BY s.initial_price_usd DESC) AS rank FROM applications_developers AS d
INNER JOIN steam_applications AS s ON s.app_id = d.app_id
WHERE s.initial_price NOT null
QUALIFY rank <=3
ORDER BY d.game_devs, rank;

-- 3. Moat popular genres and their mean price
SELECT g.description_name AS category_name, COUNT(DISTINCT(g.app_id)) AS num_games, ROUND(AVG(s.initial_price_usd/100), 2) AS average_price FROM applications_genres AS g
JOIN steam_applications AS s ON s.app_id = g.app_id
GROUP BY description_name
ORDER BY num_games DESC;

-- 4. Number of positive/negative reviews by genre
SELECT description_name, SUM(total_positive)/SUM(total_negative) AS pos_neg_ratio, SUM(total_reviews) FROM reviews AS r
JOIN applications_genres AS g ON g.app_id = r.app_id
GROUP BY description_name
HAVING TRY_CAST(pos_neg_ratio AS FLOAT) IS NOT NULL
ORDER BY pos_neg_ratio DESC;

-- 5. Top 20 languages in reviews
SELECT review_language, COUNT(app_id) num_reviews FROM reviews_comments
GROUP BY review_language
ORDER BY num_reviews DESC
LIMIT 20;

-- 6. Number of games released (or planned to release by year)
SELECT RIGHT(release_date, 4) AS released_year, COUNT(app_id) FROM steam_applications
WHERE TRY_CAST(released_year AS INT) NOT null
GROUP BY released_year
ORDER BY released_year DESC;