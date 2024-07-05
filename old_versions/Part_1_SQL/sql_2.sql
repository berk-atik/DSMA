SELECT t_b.business_id1 AS business_id
    ,business_lat
    ,business_long
    ,business_stars
    ,review_count
	,check_in_count
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
) AS t_c ON t_b.business_id1 = t_c.business_id3;
