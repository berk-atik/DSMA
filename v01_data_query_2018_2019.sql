WITH split_dates AS (
    SELECT 
        (j->> 'business_id')::text AS business_id,
        UNNEST(string_to_array((j->> 'date')::text, ', ')) AS date_str
    FROM public.checkin
),
converted_dates AS (
    SELECT 
        business_id,
        to_timestamp(date_str, 'YYYY-MM-DD HH24:MI:SS') AS date_val
    FROM split_dates
),
yearly_checkins AS (
    SELECT 
        business_id,
        EXTRACT(YEAR FROM date_val) AS year,
        COUNT(*) AS check_in_count
    FROM converted_dates
    WHERE date_val IS NOT NULL
    GROUP BY business_id, EXTRACT(YEAR FROM date_val)
),
yearly_reviews AS (
    SELECT 
        (j->> 'business_id')::text AS business_id,
        EXTRACT(YEAR FROM to_timestamp((j->> 'date')::text, 'YYYY-MM-DD HH24:MI:SS')) AS year,
        COUNT(*) AS review_count,
        ROUND(AVG((j->> 'stars')::numeric), 2) AS average_stars
    FROM public.review
    WHERE to_timestamp((j->> 'date')::text, 'YYYY-MM-DD HH24:MI:SS') IS NOT NULL
    GROUP BY (j->> 'business_id')::text, EXTRACT(YEAR FROM to_timestamp((j->> 'date')::text, 'YYYY-MM-DD HH24:MI:SS'))
),
business_data AS (
    SELECT 
        (j->> 'business_id')::text AS business_id,
        (j->> 'latitude')::numeric AS business_lat,
        (j->> 'longitude')::numeric AS business_long,
        (j->> 'postal_code') AS postal_code,
        (j->> 'stars')::numeric AS overall_business_stars,
        (j-> 'review_count')::integer AS overall_review_count,
        (LENGTH((j->> 'attributes')::text) - LENGTH(REPLACE((j->> 'attributes')::text, 'True', '')))/4 AS attribute_count,
        CASE 
            WHEN STRPOS(j->'attributes'->> 'BusinessParking', 'True') <> 0 THEN 1
            ELSE 0
        END AS business_park,
        CASE 
            WHEN STRPOS(j->'attributes'->> 'HappyHour', 'True') <> 0 THEN 1
            ELSE 0
        END AS business_happy_hour,
        (j->'attributes'->> 'RestaurantsPriceRange2') AS business_price,
        (j->> 'is_open') AS business_open
    FROM public.business
    WHERE STRPOS(j->> 'categories', 'Restaurants') <> 0 
      AND j->> 'city' = 'Philadelphia'
),
photo_counts AS (
    SELECT 
        (j->> 'business_id')::text AS business_id,
        COUNT(*) AS n_photo
    FROM public.photo
    GROUP BY (j->> 'business_id')::text
)
SELECT 
    b.business_id,
    b.business_lat,
    b.business_long,
    b.postal_code,
    b.overall_business_stars,
    b.overall_review_count,
    b.attribute_count,
    b.business_park,
    b.business_happy_hour,
    b.business_price,
    b.business_open,
    COALESCE(p.n_photo, 0) AS n_photo,
    yc.year,
    yc.check_in_count,
    COALESCE(yr.review_count, 0) AS review_count,
    COALESCE(yr.average_stars, 0.0) AS average_stars
FROM business_data b
LEFT JOIN photo_counts p ON b.business_id = p.business_id
LEFT JOIN yearly_checkins yc ON b.business_id = yc.business_id
LEFT JOIN yearly_reviews yr ON b.business_id = yr.business_id AND yc.year = yr.year
WHERE yc.year IN (2018, 2019)
ORDER BY b.business_id, yc.year;
