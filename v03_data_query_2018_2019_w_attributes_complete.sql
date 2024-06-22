-- Split the date field from the JSON in the checkin table into individual date strings
WITH split_dates AS (
    SELECT 
        (j->> 'business_id')::text AS business_id, -- Extract business_id as text
        UNNEST(string_to_array((j->> 'date')::text, ', ')) AS date_str -- Split the 'date' field into separate date strings
    FROM public.checkin
),

-- Convert the split date strings into timestamp values
converted_dates AS (
    SELECT 
        business_id, -- Business ID from the previous CTE
        to_timestamp(date_str, 'YYYY-MM-DD HH24:MI:SS') AS date_val -- Convert date strings to timestamp
    FROM split_dates
),

-- Aggregate check-in counts by business ID and year
yearly_checkins AS (
    SELECT 
        business_id, -- Business ID from the previous CTE
        EXTRACT(YEAR FROM date_val) AS year, -- Extract the year from the timestamp
        COUNT(*) AS check_in_count -- Count the number of check-ins
    FROM converted_dates
    WHERE date_val IS NOT NULL -- Exclude null timestamps
    GROUP BY business_id, EXTRACT(YEAR FROM date_val)
),

-- Aggregate review counts and average stars by business ID and year
yearly_reviews AS (
    SELECT 
        (j->> 'business_id')::text AS business_id, -- Extract business_id as text
        EXTRACT(YEAR FROM to_timestamp((j->> 'date')::text, 'YYYY-MM-DD HH24:MI:SS')) AS year, -- Extract the year from the review date
        COUNT(*) AS review_count, -- Count number of reviews
        ROUND(AVG((j->> 'stars')::numeric), 2) AS average_stars -- Calculate the mean of rating
    FROM public.review
    WHERE to_timestamp((j->> 'date')::text, 'YYYY-MM-DD HH24:MI:SS') IS NOT NULL -- Exclude null timestamps
    GROUP BY (j->> 'business_id')::text, EXTRACT(YEAR FROM to_timestamp((j->> 'date')::text, 'YYYY-MM-DD HH24:MI:SS'))
),

-- Extract and transform data from the 'business' table
-- Extract and transform data from the 'business' table
business_data AS (
    SELECT 
        (j->> 'business_id')::text AS business_id, -- Extract business_id as text
        (j->> 'name') AS name, -- Extract business name
        (j->> 'address') AS address, -- Extract business address
        (j->> 'city') AS city, -- Extract city
        (j->> 'state') AS state, -- Extract state
        (j->> 'postal_code') AS postal_code, -- Extract ZIP code
        (j->> 'latitude')::numeric AS business_lat, -- Extract latitude
        (j->> 'longitude')::numeric AS business_long, -- Extract longitude
        (j->> 'stars')::numeric AS overall_business_stars, -- Extract overall star rating
        (j-> 'review_count')::integer AS overall_review_count, -- Extract overall review count
        (j->> 'is_open')::integer AS business_open, -- Extract open status
        (j->> 'categories') AS categories, -- Extract categories
        (j->> 'hours') AS hours, -- Extract hours of operation
        REPLACE(j->'attributes'->>'Alcohol', 'u\'', '') AS Alcohol, -- Extract Alcohol
        REPLACE(j->'attributes'->>'OutdoorSeating', 'u\'', '') AS OutdoorSeating, -- Extract OutdoorSeating
        REPLACE(j->'attributes'->>'RestaurantsTableService', 'u\'', '') AS RestaurantsTableService, -- Extract RestaurantsTableService
        REPLACE(j->'attributes'->>'BikeParking', 'u\'', '') AS BikeParking, -- Extract BikeParking
        REPLACE(j->'attributes'->>'HappyHour', 'u\'', '') AS HappyHour, -- Extract HappyHour
        REPLACE(j->'attributes'->>'BYOB', 'u\'', '') AS BYOB, -- Extract Bring your own booze
        REPLACE(j->'attributes'->>'BusinessAcceptsCreditCards', 'u\'', '') AS BusinessAcceptsCreditCards, -- Extract BusinessAcceptsCreditCards
        REPLACE(j->'attributes'->>'RestaurantsCounterService', 'u\'', '') AS RestaurantsCounterService, -- Extract RestaurantsCounterService
        REPLACE(j->'attributes'->>'HasTV', 'u\'', '') AS HasTV, -- Extract if business got a tv
        REPLACE(j->'attributes'->>'RestaurantsPriceRange2', 'u\'', '') AS RestaurantsPriceRange2, -- Extract RestaurantsPriceRange
        REPLACE(j->'attributes'->>'RestaurantsReservations', 'u\'', '') AS RestaurantsReservations, -- Extract RestaurantsReservations
        REPLACE(j->'attributes'->>'RestaurantsDelivery', 'u\'', '') AS RestaurantsDelivery, -- Extract RestaurantsDelivery
        REPLACE(j->'attributes'->>'WiFi', 'u\'', '') AS WiFi, -- Extract WiFi
        REPLACE(j->'attributes'->>'RestaurantsTakeOut', 'u\'', '') AS RestaurantsTakeOut, -- Extract RestaurantsTakeOut
        j->'attributes' AS attributes_json, -- Extract attributes as JSON
        -- Clean and convert Ambience and BusinessParking attributes to JSONB
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(j->'attributes'->>'Ambience', '''', '"'), 'u"', '"'), 'False', 'false'), 'True', 'true'), 'None', 'null')::jsonb AS Ambience_jsonb,
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(j->'attributes'->>'BusinessParking', '''', '"'), 'u"', '"'), 'False', 'false'), 'True', 'true'), 'None', 'null')::jsonb AS BusinessParking_jsonb
    FROM public.business j
    WHERE STRPOS(j->> 'categories', 'Restaurants') <> 0 -- Include only businesses categorized as Restaurants
      AND j->> 'city' = 'Philadelphia' -- Include only businesses in Philadelphia
)
,

-- Count the number of photos per business
photo_counts AS (
    SELECT 
        (j->> 'business_id')::text AS business_id, -- Extract business_id as text
        COUNT(*) AS n_photo -- Count number of photos
    FROM public.photo
    GROUP BY (j->> 'business_id')::text
),

-- Combine all data into a single dataset, joining on business ID and year
yearly_data AS (
    SELECT 
        b.business_id, -- Business ID 
        b.name, -- Business name
        b.address, -- Business address
        b.city, -- Business city
        b.state, -- Business state
        b.postal_code, -- Business postal code
        b.business_lat, -- Business latitude
        b.business_long, -- Business longitude
        b.overall_business_stars, -- Overall business star rating
        b.overall_review_count, -- Overall review count
        b.business_open, -- Open status
        b.categories, -- Categories
        b.hours, -- Opening hours
        b.Alcohol, -- Alcohol attribute
        b.OutdoorSeating, -- OutdoorSeating attribute
        b.RestaurantsTableService, -- RestaurantsTableService attribute
        b.BikeParking, -- BikeParking attribute
        b.HappyHour, -- HappyHour attribute
        b.BYOB, -- BYOB attribute
        b.BusinessAcceptsCreditCards, -- BusinessAcceptsCreditCards attribute
        b.RestaurantsCounterService, -- RestaurantsCounterService attribute
        b.HasTV, -- HasTV attribute
        b.RestaurantsPriceRange2, -- RestaurantsPriceRange2 attribute
        b.RestaurantsReservations, -- RestaurantsReservations attribute
        b.RestaurantsDelivery, -- RestaurantsDelivery attribute
        b.WiFi, -- WiFi attribute
        b.RestaurantsTakeOut, -- RestaurantsTakeOut attribute
        b.attributes_json, -- Attributes JSON
        COALESCE(p.n_photo, 0) AS n_photo, -- Number of photos (default to 0 if no photos)
        yc.year, -- Year from yearly_checkins
        yc.check_in_count, -- Check-in count from yearly_checkins
        COALESCE(yr.review_count, 0) AS review_count, -- Review count (default to 0 if no reviews)
        COALESCE(yr.average_stars, 0.0) AS average_stars, -- Average star rating (default to 0.0 if no reviews)
        -- Extract Ambience attributes
        (b.Ambience_jsonb->>'romantic')::boolean AS IsRomantic,
        (b.Ambience_jsonb->>'intimate')::boolean AS IsIntimate,
        (b.Ambience_jsonb->>'touristy')::boolean AS IsTouristy,
        (b.Ambience_jsonb->>'hipster')::boolean AS IsHipster,
        (b.Ambience_jsonb->>'divey')::boolean AS IsDivey,
        (b.Ambience_jsonb->>'classy')::boolean AS IsClassy,
        (b.Ambience_jsonb->>'trendy')::boolean AS IsTrendy,
        (b.Ambience_jsonb->>'upscale')::boolean AS IsUpscale,
        (b.Ambience_jsonb->>'casual')::boolean AS IsCasual,
        -- Extract BusinessParking attributes
        (b.BusinessParking_jsonb->>'garage')::boolean AS parking_garage,
        (b.BusinessParking_jsonb->>'street')::boolean AS parking,
        (b.BusinessParking_jsonb->>'street')::boolean AS parking_street,
        (b.BusinessParking_jsonb->>'validated')::boolean AS parking_validated,
        (b.BusinessParking_jsonb->>'lot')::boolean AS parking_lot,
        (b.BusinessParking_jsonb->>'valet')::boolean AS parking_valet
    FROM business_data b
    LEFT JOIN photo_counts p ON b.business_id = p.business_id -- Join photo counts
    LEFT JOIN yearly_checkins yc ON b.business_id = yc.business_id -- Join yearly check-ins
    LEFT JOIN yearly_reviews yr ON b.business_id = yr.business_id AND yc.year = yr.year -- Join yearly reviews
    WHERE yc.year IN (2018, 2019) -- Include data for the years 2018 and 2019
)

-- Filter to include only businesses with entries for both 2018 and 2019
SELECT 
    yd.business_id, -- Business ID
    yd.name, -- Business name
    yd.address, -- Business address
    yd.city, -- Business city
    yd.state, -- Business state
    yd.postal_code, -- Business postal code
    yd.business_lat, -- Business latitude
    yd.business_long, -- Business longitude
    yd.overall_business_stars, -- Overall business star rating
    yd.overall_review_count, -- Overall review count
    yd.business_open, -- Open status
    yd.categories, -- Categories
    yd.hours, -- Business hours
    yd.IsRomantic, -- Ambience attribute: romantic
    yd.IsIntimate, -- Ambience attribute: intimate
    yd.IsTouristy, -- Ambience attribute: touristy
    yd.IsHipster, -- Ambience attribute: hipster
    yd.IsDivey, -- Ambience attribute: divey
    yd.IsClassy, -- Ambience attribute: classy
    yd.IsTrendy, -- Ambience attribute: trendy
    yd.IsUpscale, -- Ambience attribute: upscale
    yd.IsCasual, -- Ambience attribute: casual
    yd.parking_garage, -- Parking attribute: garage
    yd.parking_street, -- Parking attribute: street
    yd.parking_validated, -- Parking attribute: validated
    yd.parking_lot, -- Parking attribute: lot
    yd.parking_valet, -- Parking attribute: valet
    yd.Alcohol, -- Alcohol attribute
    yd.OutdoorSeating, -- OutdoorSeating attribute
    yd.RestaurantsTableService, -- RestaurantsTableService attribute
    yd.BikeParking, -- BikeParking attribute
    yd.HappyHour, -- HappyHour attribute
    yd.BYOB, -- BYOB attribute
    yd.BusinessAcceptsCreditCards, -- BusinessAcceptsCreditCards attribute
    yd.RestaurantsCounterService, -- RestaurantsCounterService attribute
    yd.HasTV, -- HasTV attribute
    yd.RestaurantsPriceRange2, -- RestaurantsPriceRange2 attribute
    yd.RestaurantsReservations, -- RestaurantsReservations attribute
    yd.RestaurantsDelivery, -- RestaurantsDelivery attribute
    yd.WiFi, -- WiFi attribute
    yd.RestaurantsTakeOut, -- RestaurantsTakeOut attribute
    yd.n_photo, -- Number of photos
    yd.year, -- Year
    yd.check_in_count, -- Check-in count
    yd.review_count, -- Review count
    yd.average_stars -- Average star rating
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
ORDER BY yd.business_id, yd.year; -- Order by business ID and year
