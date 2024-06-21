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
        (j->> 'name') AS name,
        (j->> 'address') AS address,
        (j->> 'city') AS city,
        (j->> 'state') AS state,
        (j->> 'postal_code') AS postal_code,
        (j->> 'latitude')::numeric AS business_lat,
        (j->> 'longitude')::numeric AS business_long,
        (j->> 'stars')::numeric AS overall_business_stars,
        (j-> 'review_count')::integer AS overall_review_count,
        (j->> 'is_open')::integer AS business_open,
        (j->> 'categories') AS categories,
        (j->> 'hours') AS hours,
        j->'attributes' AS attributes_json,
        (j->'attributes'->>'Alcohol') AS Alcohol,
        (j->'attributes'->>'OutdoorSeating') AS OutdoorSeating,
        (j->'attributes'->>'RestaurantsTableService') AS RestaurantsTableService,
        (j->'attributes'->>'BikeParking') AS BikeParking,
        (j->'attributes'->>'HappyHour') AS HappyHour,
        (j->'attributes'->>'BYOB') AS BYOB,
        (j->'attributes'->>'BusinessAcceptsCreditCards') AS BusinessAcceptsCreditCards,
        (j->'attributes'->>'Ambience') AS Ambience,
        (j->'attributes'->>'RestaurantsCounterService') AS RestaurantsCounterService,
        (j->'attributes'->>'HasTV') AS HasTV,
        (j->'attributes'->>'RestaurantsPriceRange2') AS RestaurantsPriceRange2,
        (j->'attributes'->>'RestaurantsReservations') AS RestaurantsReservations,
        (j->'attributes'->>'RestaurantsDelivery') AS RestaurantsDelivery,
        (j->'attributes'->>'WiFi') AS WiFi,
        (j->'attributes'->>'BusinessParking') AS BusinessParking,
        (j->'attributes'->>'RestaurantsTakeOut') AS RestaurantsTakeOut
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
),
yearly_data AS (
    SELECT 
        b.business_id,
        b.name,
        b.address,
        b.city,
        b.state,
        b.postal_code,
        b.business_lat,
        b.business_long,
        b.overall_business_stars,
        b.overall_review_count,
        b.business_open,
        b.categories,
        b.hours,
        b.Alcohol,
        b.OutdoorSeating,
        b.RestaurantsTableService,
        b.BikeParking,
        b.HappyHour,
        b.BYOB,
        b.BusinessAcceptsCreditCards,
        b.Ambience,
        b.RestaurantsCounterService,
        b.HasTV,
        b.RestaurantsPriceRange2,
        b.RestaurantsReservations,
        b.RestaurantsDelivery,
        b.WiFi,
        b.BusinessParking,
        b.RestaurantsTakeOut,
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
)
-- Filter to include only businesses with entries for both 2018 and 2019
SELECT 
    yd.business_id,
    yd.name,
    yd.address,
    yd.city,
    yd.state,
    yd.postal_code,
    yd.business_lat,
    yd.business_long,
    yd.overall_business_stars,
    yd.overall_review_count,
    yd.business_open,
    yd.categories,
    yd.hours,
    yd.Alcohol,
    yd.OutdoorSeating,
    yd.RestaurantsTableService,
    yd.BikeParking,
    yd.HappyHour,
    yd.BYOB,
    yd.BusinessAcceptsCreditCards,
    yd.Ambience,
    yd.RestaurantsCounterService,
    yd.HasTV,
    yd.RestaurantsPriceRange2,
    yd.RestaurantsReservations,
    yd.RestaurantsDelivery,
    yd.WiFi,
    yd.BusinessParking,
    yd.RestaurantsTakeOut,
    yd.n_photo,
    yd.year,
    yd.check_in_count,
    yd.review_count,
    yd.average_stars
FROM yearly_data yd
WHERE yd.business_id IN (
    SELECT business_id
    FROM yearly_data
    WHERE year = 2018
)
AND yd.business_id IN (
    SELECT business_id
    FROM yearly_data
    WHERE year = 2019
)
ORDER BY yd.business_id, yd.year;
