SELECT t_b.business_id1 AS business_id
    ,business_lat
    ,business_long
    ,business_stars
	,business_stars_pre_2020
    ,review_count
	,review_count_pre_2020
	,check_in_count
	,check_in_count_pre_2020
    ,business_park
    ,business_happy_h
    ,business_price
    ,business_open
    ,business_cat
    ,n_photo
    ,cat_count
FROM (
-- table1: for business

SELECT business_id AS business_id1
    ,latitude AS business_lat
    ,longitude AS business_long
    ,stars AS business_stars
    ,review_count AS review_count
    ,(
    CASE 
            WHEN STRPOS((attributes::json) ->> 'BusinessParking', 'True') <> 0
                THEN '1' --true
            ELSE '0' --false
            END
        ) AS business_park
    ,(
    CASE 
            WHEN STRPOS((attributes::json) ->> 'HappyHour', 'True') <> 0
                THEN '1' --true
            ELSE '0' --false
            END
        ) AS business_happy_h
    ,CAST((attributes::json) ->> 'RestaurantsPriceRange2' AS INTEGER) AS business_price
    ,is_open AS business_open
    ,categories AS business_cat
    ,LENGTH(categories) - LENGTH(REPLACE(categories, ',', '')) + 1 AS cat_count --to count number of categories
FROM public3.businesstable
WHERE STRPOS(categories, 'Restaurants') <> 0 AND city='Philadelphia'
) AS t_b
LEFT JOIN (
-- table2: for photos

SELECT business_id AS business_id2
   	 ,COUNT(*) AS n_photo
FROM public3.phototable
GROUP BY business_id
) AS t_p ON t_b.business_id1 = t_p.business_id2
LEFT JOIN (
-- table3: for check-ins

SELECT business_id AS business_id3
    ,SUM(LENGTH(date) - LENGTH(REPLACE(date, ',', '')) + 1) AS check_in_count -- to count number of check-ins
FROM public3.checkintable
GROUP BY business_id
) AS t_c ON t_b.business_id1 = t_c.business_id3
LEFT JOIN (
--table4: for check-ins before 2020

WITH split_dates AS (
    SELECT business_id 
        ,UNNEST(string_to_array(date, ', ')) AS date_str
    FROM public3.checkintable
),
converted_dates AS (
    SELECT business_id
        ,to_timestamp(date_str, 'YYYY-MM-DD HH24:MI:SS') AS date_val
    FROM 
        split_dates
)
SELECT business_id AS business_id4 
    ,COUNT(*) AS check_in_count_pre_2020
	FROM converted_dates
WHERE date_val < '2020-01-01' -- before COVID condition
GROUP BY business_id
) AS t_c2 ON t_b.business_id1 = t_c2.business_id4
LEFT JOIN (
--table 5: for average stars before 2020

SELECT business_id AS business_id5 
	,SUM(stars) AS sum_of_stars
	,COUNT(*) AS review_count_pre_2020
	,ROUND(SUM(stars) / COUNT(*), 1) AS business_stars_pre_2020
FROM public3.reviewtable
WHERE date < '2020-01-01' -- before COVID condition
GROUP BY business_id
) AS t_r ON t_b.business_id1 = t_r.business_id5;

