WITH business_data AS (
    SELECT (j->> 'business_id')::text AS business_id1,
           (j->> 'latitude')::numeric AS business_lat,
           (j->> 'longitude')::numeric AS business_long,
           (j->> 'postal_code') AS postal_code,
           (j->> 'stars')::numeric AS business_stars,
           (j-> 'review_count')::integer AS review_count,
           (LENGTH((j->> 'attributes')::text) - LENGTH(REPLACE((j->> 'attributes')::text, 'True', '')))/4 AS attribute_count,
           CASE 
               WHEN STRPOS(j->'attributes'->> 'BusinessParking', 'True') <> 0 THEN 1
               ELSE 0
           END AS business_park,
           CASE 
               WHEN STRPOS(j->'attributes'->> 'HappyHour', 'True') <> 0 THEN 1
               ELSE 0
           END AS business_happy_h,
           (j->'attributes'->> 'RestaurantsPriceRange2') AS business_price,
           (j->> 'is_open') AS business_open
    FROM public.business
    WHERE STRPOS(j->> 'categories', 'Restaurants') <> 0 AND j->> 'city' = 'Philadelphia'
),
photo_counts AS (
    SELECT (j->> 'business_id')::text AS business_id2,
           COUNT(*) AS n_photo
    FROM public.photo
    GROUP BY (j->> 'business_id')::text
),
checkin_counts AS (
    SELECT (j->> 'business_id')::text AS business_id3,
           LENGTH((j->> 'date')::text) - LENGTH(REPLACE((j->> 'date')::text, ',', '')) + 1 AS check_in_count
    FROM public.checkin
),
checkin_counts_pre_2020 AS (
    WITH split_dates AS (
        SELECT (j->> 'business_id')::text AS business_id,
               UNNEST(string_to_array((j->> 'date')::text, ', ')) AS date_str
        FROM public.checkin
    ),
    converted_dates AS (
        SELECT business_id,
               to_timestamp(date_str, 'YYYY-MM-DD HH24:MI:SS') AS date_val
        FROM split_dates
    )
    SELECT business_id AS business_id4,
           COUNT(*) AS check_in_count_pre_2020
    FROM converted_dates
    WHERE date_val < '2020-01-01'
    GROUP BY business_id
),
review_stats_pre_2020 AS (
    SELECT (j->> 'business_id')::text AS business_id5,
           SUM((j->> 'stars')::numeric) AS sum_of_stars,
           COUNT(*) AS review_count_pre_2020,
           ROUND(SUM((j->> 'stars')::numeric) / COUNT(*), 1) AS business_stars_pre_2020
    FROM public.review
    WHERE (j->> 'date')::timestamp without time zone < '2020-01-01'
    GROUP BY (j->> 'business_id')::text
)
SELECT t_b.business_id1 AS business_id,
       business_lat,
       business_long,
       postal_code,
       business_stars,
       business_stars_pre_2020,
       review_count,
       review_count_pre_2020,
       check_in_count,
       check_in_count_pre_2020,
       attribute_count,
       business_park,
       business_happy_h,
       business_price,
       business_open,
       n_photo
FROM business_data AS t_b
LEFT JOIN photo_counts AS t_p ON t_b.business_id1 = t_p.business_id2
LEFT JOIN checkin_counts AS t_c ON t_b.business_id1 = t_c.business_id3
LEFT JOIN checkin_counts_pre_2020 AS t_c2 ON t_b.business_id1 = t_c2.business_id4
LEFT JOIN review_stats_pre_2020 AS t_r ON t_b.business_id1 = t_r.business_id5
LIMIT 10;
