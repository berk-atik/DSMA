-- Extract and unnest dates from the checkin table
WITH split_dates AS (
    SELECT 
        (j->> 'business_id')::text AS business_id,   -- Extract business_id as text
        UNNEST(string_to_array((j->> 'date')::text, ', ')) AS date_str  -- Split and unnest the dates
    FROM public.checkin
),

-- Convert the split dates to timestamp format
converted_dates AS (
    SELECT 
        business_id,
        to_timestamp(date_str, 'YYYY-MM-DD HH24:MI:SS') AS date_val  -- Convert string dates to timestamp
    FROM split_dates
),

-- Aggregate check-in counts by year
yearly_checkins AS (
    SELECT 
        business_id,
        EXTRACT(YEAR FROM date_val) AS year,  -- Extract the year from the timestamp
        COUNT(*) AS check_in_count  -- Count the number of check-ins per year
    FROM converted_dates
    WHERE date_val IS NOT NULL  -- Ensure date is not null
    GROUP BY business_id, EXTRACT(YEAR FROM date_val)
),

-- Aggregate review counts and average stars by year
yearly_reviews AS (
    SELECT 
        (j->> 'business_id')::text AS business_id,  -- Extract business_id as text
        EXTRACT(YEAR FROM to_timestamp((j->> 'date')::text, 'YYYY-MM-DD HH24:MI:SS')) AS year,  -- Extract year from review date
        COUNT(*) AS review_count,  -- Count the number of reviews per year
        ROUND(AVG((j->> 'stars')::numeric), 2) AS average_stars  -- Calculate average stars per year
    FROM public.review
    WHERE to_timestamp((j->> 'date')::text, 'YYYY-MM-DD HH24:MI:SS') IS NOT NULL  -- Ensure date is not null
    GROUP BY (j->> 'business_id')::text, EXTRACT(YEAR FROM to_timestamp((j->> 'date')::text, 'YYYY-MM-DD HH24:MI:SS'))
),

-- Extract business details including specific attributes from the JSON
business_data AS (
    SELECT 
        (j->> 'business_id')::text AS business_id,  -- Extract business_id
        (j->> 'name') AS name,  -- Extract name
        (j->> 'address') AS address,  -- Extract address
        (j->> 'city') AS city,  -- Extract city
        (j->> 'state') AS state,  -- Extract state
        (j->> 'postal_code') AS postal_code,  -- Extract postal code
        (j->> 'latitude')::numeric AS business_lat,  -- Extract latitude and convert to numeric
        (j->> 'longitude')::numeric AS business_long,  -- Extract longitude and convert to numeric
        (j->> 'stars')::numeric AS overall_business_stars,  -- Extract overall stars and convert to numeric
        (j-> 'review_count')::integer AS overall_review_count,  -- Extract overall review count and convert to integer
        (j->> 'is_open')::integer AS business_open,  -- Extract business open status and convert to integer
        (j->> 'categories') AS categories,  -- Extract categories
        (j->> 'hours') AS hours,  -- Extract hours
        j->'attributes' AS attributes_json,  -- Extract the entire attributes JSON
        (j->'attributes'->>'Alcohol') AS Alcohol,  -- Extract specific attributes
        (j->'attributes'->>'OutdoorSeating') AS OutdoorSeating,
        (j->'attributes'->>'RestaurantsTableService') AS RestaurantsTableService,
        (j->'attributes'->>'BikeParking') AS BikeParking,
        (j->'attributes'->>'HappyHour') AS HappyHour,
        (j->'attributes'->>'BYOB') AS BYOB,
        (j->'attributes'->>'BusinessAcceptsCreditCards') AS BusinessAcceptsCreditCards,
        (j->'attributes'->>'RestaurantsCounterService') AS RestaurantsCounterService,
        (j->'attributes'->>'HasTV') AS HasTV,
        (j->'attributes'->>'RestaurantsPriceRange2') AS RestaurantsPriceRange2,
        (j->'attributes'->>'RestaurantsReservations') AS RestaurantsReservations,
        (j->'attributes'->>'RestaurantsDelivery') AS RestaurantsDelivery,
        (j->'attributes'->>'WiFi') AS WiFi,
        (j->'attributes'->>'BusinessParking') AS BusinessParking,
        (j->'attributes'->>'RestaurantsTakeOut') AS RestaurantsTakeOut,
        -- Extract and rename Ambience attributes
        (j->'attributes'->'Ambience'->>'romantic')::boolean AS IsRomantic,
        (j->'attributes'->'Ambience'->>'intimate')::boolean AS IsIntimate,
        (j->'attributes'->'Ambience'->>'touristy')::boolean AS IsTouristy,
        (j->'attributes'->'Ambience'->>'hipster')::boolean AS IsHipster,
        (j->'attributes'->'Ambience'->>'divey')::boolean AS IsDivey,
        (j->'attributes'->'Ambience'->>'classy')::boolean AS IsClassy,
        (j->'attributes'->'Ambience'->>'trendy')::boolean AS IsTrendy,
        (j->'attributes'->'Ambience'->>'upscale')::boolean AS IsUpscale,
        (j->'attributes'->'Ambience'->>'casual')::boolean AS IsCasual,
        -- Extract and rename BusinessParking attributes
        (j->'attributes'->'BusinessParking'->>'garage')::boolean AS parking_garage,
        (j->'attributes'->'BusinessParking'->>'street')::boolean AS parking_street,
        (j->'attributes'->'BusinessParking'->>'validated')::boolean AS parking_validated,
        (j->'attributes'->'BusinessParking'->>'lot')::boolean AS parking_lot,
        (j->'attributes'->'BusinessParking'->>'valet')::boolean AS parking_valet
    FROM public.business
    WHERE STRPOS(j->> 'categories', 'Restaurants') <> 0  -- Filter for businesses in the Restaurants category
      AND j->> 'city' = 'Philadelphia'  -- Filter for businesses in Philadelphia
),

-- Aggregate photo counts by business
photo_counts AS (
    SELECT 
        (j->> 'business_id')::text AS business_id,  -- Extract business_id
        COUNT(*) AS n_photo  -- Count the number of photos
    FROM public.photo
    GROUP BY (j->> 'business_id')::text
),

-- Combine business data with check-ins, reviews, and photo counts
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
        b.RestaurantsCounterService,
        b.HasTV,
        b.RestaurantsPriceRange2,
        b.RestaurantsReservations,
        b.RestaurantsDelivery,
        b.WiFi,
        b.BusinessParking,
        b.RestaurantsTakeOut,
        b.IsRomantic,
        b.IsIntimate,
        b.IsTouristy,
        b.IsHipster,
        b.IsDivey,
        b.IsClassy,
        b.IsTrendy,
        b.IsUpscale,
        b.IsCasual,
        b.parking_garage,
        b.parking_street,
        b.parking_validated,
        b.parking_lot,
        b.parking_valet,
        COALESCE(p.n_photo, 0) AS n_photo,  -- Handle cases where photo count is null
        yc.year,
        yc.check_in_count,
        COALESCE(yr.review_count, 0) AS review_count,  -- Handle cases where review count is null
        COALESCE(yr.average_stars, 0.0) AS average_stars  -- Handle cases where average stars is null
    FROM business_data b
    LEFT JOIN photo_counts p ON b.business_id = p.business_id  -- Join with photo counts
    LEFT JOIN yearly_checkins yc ON b.business_id = yc.business_id  -- Join with yearly check-ins
    LEFT JOIN yearly_reviews yr ON b.business_id = yr.business_id AND yc.year = yr.year  -- Join with yearly reviews
    WHERE yc.year IN (2018, 2019)  -- Filter for years 2018 and 2019
)

-- Final selection of businesses with entries for both 2018 and 2019
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
    yd.RestaurantsCounterService,
    yd.HasTV,
    yd.RestaurantsPriceRange2,
    yd.RestaurantsReservations,
    yd.RestaurantsDelivery,
    yd.WiFi,
    yd.BusinessParking,
    yd.RestaurantsTakeOut,
    yd.IsRomantic,
    yd.IsIntimate,
    yd.IsTouristy,
    yd.IsHipster,
    yd.IsDivey,
    yd.IsClassy,
    yd.IsTrendy,
    yd.IsUpscale,
    yd.IsCasual,
    yd.parking_garage,
    yd.parking_street,
    yd.parking_validated,
    yd.parking_lot,
    yd.parking_valet,
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
ORDER BY yd.business_id, yd.year;  -- Order by business_id and year
