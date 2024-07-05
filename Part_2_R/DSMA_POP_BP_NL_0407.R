# Load necessary packages ---------------------------------------
load_or_install <- function(package) {
  if (!require(package, character.only = TRUE)) {
    install.packages(package, dependencies = TRUE)
    library(package, character.only = TRUE)
  }
}

packages <- c("readr", "dplyr", "tidyr", "geosphere", "ggplot2", 
              "cowplot", "parallel", "leaflet", "sf", "osmdata", "dbscan", "stringr", "data.table", "pbapply", "mice")
lapply(packages, load_or_install)

# Standardize ZIP code format function
standardize_zip <- function(zip) {
  str_pad(as.character(zip), width = 5, side = "left", pad = "0")
}

# Load and transform ACS data
load_and_transform_acs <- function(file_path) {
  fread(file_path) %>%
    .[, zip_code := standardize_zip(gsub("ZCTA5 ", "", Group))]
}

# Load the Yelp data --------------------------------------------
yelp_internal_data <- fread("/Users/helua1/Desktop/Master/2. Semester/DSMA/Script/sql_9_JSON_2018_2019_w_ac.csv")

# Tidy the data -------------------------------------
yelp_internal_data_wide <- dcast(yelp_internal_data, ... ~ year, value.var = c("average_stars", "review_count", "check_in_count"))

# Load ACS data for 2018 and 2019 ---------------------------------
acs_2018 <- load_and_transform_acs("/Users/helua1/Desktop/Master/2. Semester/DSMA/Population2018.csv")
acs_2019 <- load_and_transform_acs("/Users/helua1/Desktop/Master/2. Semester/DSMA/Population2019.csv")

# Aggregation function
aggregate_age_groups <- function(data, year) {
  data[, `:=`(
    children = under_5 + `5_9` + `10_14`,
    youth = `15_19` + `20_24`,
    adults = `25_34` + `35_44` + `45_54` + `55_59` + `60_64`,
    elders = `65_74` + `75_84` + over_85
  )]
  data <- data[, .(
    year = year,
    Total.population = sum(`Total population`, na.rm = TRUE),
    children = sum(children, na.rm = TRUE),
    youth = sum(youth, na.rm = TRUE),
    adults = sum(adults, na.rm = TRUE),
    elders = sum(elders, na.rm = TRUE)
  ), by = zip_code]
  return(data)
}

# Timing the aggregation (checking processing time)
system.time({
  aggregate_age_groups_2018 <- aggregate_age_groups(acs_2018, 2018)
  aggregate_age_groups_2019 <- aggregate_age_groups(acs_2019, 2019)
})

# Save the aggregated data
fwrite(aggregate_age_groups_2018, "/Users/helua1/Desktop/Master/2. Semester/DSMA/Population2018_aggregated.csv")
fwrite(aggregate_age_groups_2019, "/Users/helua1/Desktop/Master/2. Semester/DSMA/Population2019_aggregated.csv")

# Plotting functions
create_long_format <- function(data, year) {
  data %>%
    pivot_longer(cols = c(children, youth, adults, elders),
                 names_to = "age_group",
                 values_to = "population") %>%
    mutate(year = year)
}

acs_2018_long <- create_long_format(aggregate_age_groups_2018, 2018)
acs_2019_long <- create_long_format(aggregate_age_groups_2019, 2019)

plot_data <- function(data, title) {
  ggplot(data, aes(x = reorder(zip_code, -population), y = population, fill = age_group)) +
    geom_bar(stat = "identity", position = "stack") +
    coord_flip() +
    labs(title = title, x = "ZIP Code", y = "Population") +
    scale_fill_brewer(palette = "Set3") +
    scale_y_continuous(labels = scales::comma) +
    theme(axis.text.x = element_text(angle = 90, hjust = 1))
}

plot_data(acs_2018_long, "Distribution of Age Groups by ZIP Code (2018)")
plot_data(acs_2019_long, "Distribution of Age Groups by ZIP Code (2019)")

# Load the GeoJSON file for ZIP code boundaries
zip_geojson_path <- "/Users/helua1/Desktop/Master/2. Semester/DSMA/Zipcodes_Poly.geojson"
zip_boundaries <- st_read(zip_geojson_path) %>%
  rename(zip_code = CODE)

# Convert zip_code in aggregated_acs_2018 and aggregated_acs_2019 to character
aggregated_acs_2018 <- aggregate_age_groups_2018[, zip_code := as.character(zip_code)]
aggregated_acs_2019 <- aggregate_age_groups_2019[, zip_code := as.character(zip_code)]

# Merge the ZIP code boundaries with the ACS data
zip_data_2018 <- merge(zip_boundaries, aggregated_acs_2018, by = "zip_code")
zip_data_2019 <- merge(zip_boundaries, aggregated_acs_2019, by = "zip_code")

# Function to calculate population segments for surrounding zones
calculate_population_segments_for_surrounding_zones <- function(business_zip, zip_boundaries, aggregated_acs_data) {
  surrounding_zips <- zip_boundaries %>%
    filter(lengths(st_intersects(st_geometry(zip_boundaries), st_geometry(zip_boundaries[zip_boundaries$zip_code == business_zip, ]))) > 0)
  total_surrounding_children <- sum(aggregated_acs_data$children[aggregated_acs_data$zip_code %in% surrounding_zips$zip_code], na.rm = TRUE)
  total_surrounding_youth <- sum(aggregated_acs_data$youth[aggregated_acs_data$zip_code %in% surrounding_zips$zip_code], na.rm = TRUE)
  total_surrounding_adults <- sum(aggregated_acs_data$adults[aggregated_acs_data$zip_code %in% surrounding_zips$zip_code], na.rm = TRUE)
  total_surrounding_elders <- sum(aggregated_acs_data$elders[aggregated_acs_data$zip_code %in% surrounding_zips$zip_code], na.rm = TRUE)
  return(list(surrounding_children = total_surrounding_children, surrounding_youth = total_surrounding_youth, surrounding_adults = total_surrounding_adults, surrounding_elders = total_surrounding_elders))
}

# Process the population segments for 2018 and 2019
process_population_segments_by_year <- function(yelp_sf, zip_boundaries, aggregated_acs_data) {
  num_cores <- detectCores() - 1
  yelp_chunks <- split(yelp_sf, cut(seq(nrow(yelp_sf)), num_cores, labels = FALSE))
  cl <- makeCluster(num_cores)
  clusterEvalQ(cl, {
    library(sf)
    library(dplyr)
    library(tidyr)
  })
  clusterExport(cl, c("zip_boundaries", "aggregated_acs_data", "calculate_population_segments_for_surrounding_zones", "st_intersects", "st_geometry"), envir = environment())
  
  # Use pbapply for progress bar and tryCatch for error handling (again for analysis, cause i run into errors)
  results_list <- pblapply(yelp_chunks, function(chunk) {
    tryCatch({
      chunk <- chunk %>%
        rowwise() %>%
        mutate(population_segments = list(calculate_population_segments_for_surrounding_zones(as.character(postal_code), zip_boundaries, aggregated_acs_data))) %>%
        unnest_wider(population_segments) %>%
        ungroup()
      return(chunk)
    }, error = function(e) {
      message(paste("Error processing chunk:", e))
      return(NULL)
    })
  }, cl = cl)
  
  stopCluster(cl)
  results_list <- results_list[!sapply(results_list, is.null)]  # Remove NULL results
  yelp_data_with_population_segments <- do.call(rbind, results_list)
  return(yelp_data_with_population_segments)
}

# Process for each year separately with debug information
process_and_debug <- function(year, aggregated_acs_data) {
  tryCatch({
    system.time({
      result <- process_population_segments_by_year(yelp_sf, zip_boundaries, aggregated_acs_data)
    })
    return(result)
  }, error = function(e) {
    message(paste("Error processing year", year, ":", e))
    return(NULL)
  })
}

# Load the Yelp data and convert to spatial points
yelp_internal_data_wide <- read_csv("/Users/helua1/Desktop/Master/2. Semester/DSMA/Script/merged_yelp_data_with_acs.csv")
yelp_sf <- st_as_sf(yelp_internal_data_wide, coords = c("business_long", "business_lat"), crs = 4326)

# Process 2018
message("Processing 2018")
yelp_data_with_population_segments_2018 <- process_and_debug(2018, aggregated_acs_2018)

# Process 2019
message("Processing 2019")
yelp_data_with_population_segments_2019 <- process_and_debug(2019, aggregated_acs_2019)

# Check if the results are NULL
if (is.null(yelp_data_with_population_segments_2018) | is.null(yelp_data_with_population_segments_2019)) {
  stop("Processing failed for one of the years")
}

# Select necessary columns from 2018 and 2019 datasets
population_segments_2018 <- yelp_data_with_population_segments_2018 %>%
  select(business_id, surrounding_children_2018 = surrounding_children, surrounding_youth_2018 = surrounding_youth, surrounding_adults_2018 = surrounding_adults, surrounding_elders_2018 = surrounding_elders)

population_segments_2019 <- yelp_data_with_population_segments_2019 %>%
  select(business_id, surrounding_children_2019 = surrounding_children, surrounding_youth_2019 = surrounding_youth, surrounding_adults_2019 = surrounding_adults, surrounding_elders_2019 = surrounding_elders)

# Merge the 2018 population segments
yelp_data_with_population_segments <- yelp_internal_data_wide %>%
  left_join(population_segments_2018, by = "business_id")

# Merge the 2019 population segments
yelp_data_with_population_segments <- yelp_data_with_population_segments %>%
  left_join(population_segments_2019, by = "business_id")

fwrite(yelp_data_with_population_segments, "/Users/helua1/Desktop/Master/2. Semester/DSMA/yelp_data_with_population_segments_18_19.csv")
fwrite(yelp_data_with_population_segments, "/Users/helua1/Desktop/Master/2. Semester/DSMA/yelp_data_with_population_segments_18_19_backup.csv")

# Create color palettes for each population segment for each year
pal_children_2018 <- colorNumeric(palette = "Blues", domain = yelp_data_with_population_segments$surrounding_children_2018)
pal_youth_2018 <- colorNumeric(palette = "Greens", domain = yelp_data_with_population_segments$surrounding_youth_2018)
pal_adults_2018 <- colorNumeric(palette = "Oranges", domain = yelp_data_with_population_segments$surrounding_adults_2018)
pal_elders_2018 <- colorNumeric(palette = "Reds", domain = yelp_data_with_population_segments$surrounding_elders_2018)

pal_children_2019 <- colorNumeric(palette = "Blues", domain = yelp_data_with_population_segments$surrounding_children_2019)
pal_youth_2019 <- colorNumeric(palette = "Greens", domain = yelp_data_with_population_segments$surrounding_youth_2019)
pal_adults_2019 <- colorNumeric(palette = "Oranges", domain = yelp_data_with_population_segments$surrounding_adults_2019)
pal_elders_2019 <- colorNumeric(palette = "Reds", domain = yelp_data_with_population_segments$surrounding_elders_2019)

# Extract coordinates from the sf object
coords <- st_coordinates(yelp_sf)

# Create the leaflet map with layer controls for year switching
leaflet(yelp_data_with_population_segments) %>%
  addTiles() %>%
  addCircleMarkers(
    lng = coords[, 1],
    lat = coords[, 2],
    color = ~pal_children_2018(surrounding_children_2018),
    radius = 5,
    fillOpacity = 0.7,
    group = "Children 2018",
    popup = ~paste("Business ID:", business_id, "<br>Surrounding Children 2018:", surrounding_children_2018)
  ) %>%
  addCircleMarkers(
    lng = coords[, 1],
    lat = coords[, 2],
    color = ~pal_youth_2018(surrounding_youth_2018),
    radius = 5,
    fillOpacity = 0.7,
    group = "Youth 2018",
    popup = ~paste("Business ID:", business_id, "<br>Surrounding Youth 2018:", surrounding_youth_2018)
  ) %>%
  addCircleMarkers(
    lng = coords[, 1],
    lat = coords[, 2],
    color = ~pal_adults_2018(surrounding_adults_2018),
    radius = 5,
    fillOpacity = 0.7,
    group = "Adults 2018",
    popup = ~paste("Business ID:", business_id, "<br>Surrounding Adults 2018:", surrounding_adults_2018)
  ) %>%
  addCircleMarkers(
    lng = coords[, 1],
    lat = coords[, 2],
    color = ~pal_elders_2018(surrounding_elders_2018),
    radius = 5,
    fillOpacity = 0.7,
    group = "Elders 2018",
    popup = ~paste("Business ID:", business_id, "<br>Surrounding Elders 2018:", surrounding_elders_2018)
  ) %>%
  addCircleMarkers(
    lng = coords[, 1],
    lat = coords[, 2],
    color = ~pal_children_2019(surrounding_children_2019),
    radius = 5,
    fillOpacity = 0.7,
    group = "Children 2019",
    popup = ~paste("Business ID:", business_id, "<br>Surrounding Children 2019:", surrounding_children_2019)
  ) %>%
  addCircleMarkers(
    lng = coords[, 1],
    lat = coords[, 2],
    color = ~pal_youth_2019(surrounding_youth_2019),
    radius = 5,
    fillOpacity = 0.7,
    group = "Youth 2019",
    popup = ~paste("Business ID:", business_id, "<br>Surrounding Youth 2019:", surrounding_youth_2019)
  ) %>%
  addCircleMarkers(
    lng = coords[, 1],
    lat = coords[, 2],
    color = ~pal_adults_2019(surrounding_adults_2019),
    radius = 5,
    fillOpacity = 0.7,
    group = "Adults 2019",
    popup = ~paste("Business ID:", business_id, "<br>Surrounding Adults 2019:", surrounding_adults_2019)
  ) %>%
  addCircleMarkers(
    lng = coords[, 1],
    lat = coords[, 2],
    color = ~pal_elders_2019(surrounding_elders_2019),
    radius = 5,
    fillOpacity = 0.7,
    group = "Elders 2019",
    popup = ~paste("Business ID:", business_id, "<br>Surrounding Elders 2019:", surrounding_elders_2019)
  ) %>%
  addLayersControl(
    baseGroups = c("Children 2018", "Youth 2018", "Adults 2018", "Elders 2018", "Children 2019", "Youth 2019", "Adults 2019", "Elders 2019"),
    options = layersControlOptions(collapsed = FALSE)
  ) %>%
  addLegend(pal = pal_children_2018, values = ~surrounding_children_2018, opacity = 0.7, title = "Surrounding Children 2018", position = "bottomright", group = "Children 2018") %>%
  addLegend(pal = pal_youth_2018, values = ~surrounding_youth_2018, opacity = 0.7, title = "Surrounding Youth 2018", position = "bottomright", group = "Youth 2018") %>%
  addLegend(pal = pal_adults_2018, values = ~surrounding_adults_2018, opacity = 0.7, title = "Surrounding Adults 2018", position = "bottomright", group = "Adults 2018") %>%
  addLegend(pal = pal_elders_2018, values = ~surrounding_elders_2018, opacity = 0.7, title = "Surrounding Elders 2018", position = "bottomright", group = "Elders 2018") %>%
  addLegend(pal = pal_children_2019, values = ~surrounding_children_2019, opacity = 0.7, title = "Surrounding Children 2019", position = "bottomleft", group = "Children 2019") %>%
  addLegend(pal = pal_youth_2019, values = ~surrounding_youth_2019, opacity = 0.7, title = "Surrounding Youth 2019", position = "bottomleft", group = "Youth 2019") %>%
  addLegend(pal = pal_adults_2019, values = ~surrounding_adults_2019, opacity = 0.7, title = "Surrounding Adults 2019", position = "bottomleft", group = "Adults 2019") %>%
  addLegend(pal = pal_elders_2019, values = ~surrounding_elders_2019, opacity = 0.7, title = "Surrounding Elders 2019", position = "bottomleft", group = "Elders 2019")

# Business Proximity with 5km radius -----------------------------
# Load dataset with population segments
yelp_data_with_population_segments <- read_csv("/Users/helua1/Desktop/Master/2. Semester/DSMA/yelp_data_with_population_segments_18_19.csv")


# Function to calculate Haversine distance
haversine_distance <- function(lat1, lon1, lat2, lon2) {
  dist <- distHaversine(c(lon1, lat1), c(lon2, lat2))
  return(dist / 1000)  # Convert meters to kilometers
}

# Function to calculate category overlap
calculate_overlap <- function(categories_i, categories_j) {
  cat_i <- unlist(strsplit(categories_i, ",\\s*"))
  cat_j <- unlist(strsplit(categories_j, ",\\s*"))
  common_categories <- length(intersect(cat_i, cat_j))
  total_categories_i <- length(cat_i)
  overlap <- common_categories / total_categories_i
  return(overlap)
}

# Function to calculate business proximity scores within given range
calculate_proximity <- function(chunk, yelp_data, range_km) {
  chunk %>%
    rowwise() %>%
    mutate(
      lat_i = yelp_data$business_lat[i],
      lon_i = yelp_data$business_long[i],
      lat_j = yelp_data$business_lat[j],
      lon_j = yelp_data$business_long[j],
      categories_i = yelp_data$categories[i],
      categories_j = yelp_data$categories[j],
      distance = haversine_distance(lat_i, lon_i, lat_j, lon_j),
      overlap = calculate_overlap(categories_i, categories_j)
    ) %>%
    filter(distance > 0 & distance <= range_km) %>%
    mutate(business_proximity = overlap / distance)
}

# Define the range (5 km based on literature)
range_km <- 5

# Detect the number of cores
num_cores <- detectCores() - 1  # Use one less core than the total available

# Check if the pairwise distances file exists to avoid unneccessary computing
pairwise_distances_file <- "/Users/helua1/Desktop/Master/2. Semester/DSMA/pairwise_distances.csv"
if (!file.exists(pairwise_distances_file)) {
  # Create pairwise combinations
  pairs <- expand.grid(i = 1:nrow(yelp_data_with_population_segments), j = 1:nrow(yelp_data_with_population_segments)) %>%
    filter(i < j)
  
  # Split the pairs into chunks for parallel processing (thanks for that) 
  pair_chunks <- split(pairs, cut(1:nrow(pairs), num_cores, labels = FALSE))
  
  # Set up parallel processing
  cl <- makeCluster(num_cores)
  clusterExport(cl, c("yelp_data_with_population_segments", "haversine_distance", "calculate_overlap", "calculate_proximity", "range_km"))
  clusterEvalQ(cl, library(dplyr))
  clusterEvalQ(cl, library(geosphere))
  
  # Run the proximity calculation in parallel
  results_list <- parLapply(cl, pair_chunks, function(chunk) {
    calculate_proximity(chunk, yelp_data_with_population_segments, range_km)
  })
  
  # Stop the cluster
  stopCluster(cl)
  
  # Combine the results from all chunks
  pairwise_distances <- bind_rows(results_list)
  
  # Save the pairwise distances for future use
  write_csv(pairwise_distances, pairwise_distances_file)
} else {
  # Read the existing pairwise distances file
  pairwise_distances <- read_csv(pairwise_distances_file)
}

# Summarize the business proximity scores for each business
all_business_proximity_scores <- pairwise_distances %>%
  group_by(business_id_i = yelp_data_with_population_segments$business_id[pairwise_distances$i]) %>%
  summarise(business_proximity_score = sum(business_proximity, na.rm = TRUE))

# Remove Inf values if there are any (wasn't the case) 
all_business_proximity_scores <- all_business_proximity_scores %>%
  filter(is.finite(business_proximity_score))

# Calculate the 99th percentile of business proximity scores
percentile_99 <- quantile(all_business_proximity_scores$business_proximity_score, 0.99)

# Cap the business proximity scores at the 99th percentile
all_business_proximity_scores <- all_business_proximity_scores %>%
  mutate(capped_business_proximity = pmin(business_proximity_score, percentile_99))

# Function to apply robust scaling
robust_scaler <- function(x) {
  median_x <- median(x)
  iqr_x <- IQR(x)
  scaled_x <- (x - median_x) / iqr_x
  return(scaled_x)
}

# Apply robust scaling to the capped business proximity scores
all_business_proximity_scores <- all_business_proximity_scores %>%
  mutate(scaled_capped_business_proximity = robust_scaler(capped_business_proximity))

# Remove non-finite values (again none in the final run) 
all_business_proximity_scores <- all_business_proximity_scores %>%
  filter(is.finite(scaled_capped_business_proximity))

# Merge the SCBP back into the dataset with population segments
yelp_data_with_population_and_bp <- yelp_data_with_population_segments %>%
  left_join(all_business_proximity_scores %>%
              select(business_id_i, scaled_capped_business_proximity),
            by = c("business_id" = "business_id_i"))

# Save the dataset with business proximity
write_csv(yelp_data_with_population_and_bp, "/Users/helua1/Desktop/Master/2. Semester/DSMA/yelp_internal_data_wide_with_population_segments_and_bp.csv")

# Verify merge
print(names(yelp_data_with_population_and_bp))
print(head(yelp_data_with_population_and_bp))

# Plot BP histograms ------
p1 <- ggplot(all_business_proximity_scores, aes(x = business_proximity_score)) +
  geom_histogram(binwidth = 0.5, fill = "blue", color = "black") +
  ggtitle("Original Business Proximity Scores") +
  xlab("Business Proximity Score") +
  ylab("Count")

p2 <- ggplot(all_business_proximity_scores, aes(x = capped_business_proximity)) +
  geom_histogram(binwidth = 0.5, fill = "blue", color = "black") +
  ggtitle("Capped Business Proximity Scores") +
  xlab("Capped Business Proximity Score") +
  ylab("Count")

p3 <- ggplot(all_business_proximity_scores, aes(x = scaled_capped_business_proximity)) +
  geom_histogram(binwidth = 0.5, fill = "blue", color = "black") +
  ggtitle("Scaled Capped Business Proximity Scores") +
  xlab("Scaled Capped Business Proximity Score") +
  ylab("Count")

# Combine the plots into a single figure
combined_plot <- plot_grid(p1, p2, p3, labels = c("A", "B", "C"), ncol = 3)

# Save the combined figure to a file
ggsave("compiled_business_proximity_histograms.png", plot = combined_plot, width = 15, height = 5)

# Display the combined plot
print(combined_plot)

# Visualize the geographic distribution of scaled capped business proximities ----
ggplot(yelp_data_with_population_and_bp, aes(x = business_long, y = business_lat, color = scaled_capped_business_proximity)) +
  geom_point() +
  ggtitle("Geographic Distribution of Scaled Capped Business Proximities") +
  xlab("Longitude") +
  ylab("Latitude") +
  scale_color_gradient(low = "blue", high = "red")

# Create an interactive map with leaflet 
leaflet(yelp_data_with_population_and_bp) %>%
  addTiles() %>%
  addCircleMarkers(
    lng = ~business_long,
    lat = ~business_lat,
    color = ~colorQuantile("YlOrRd", scaled_capped_business_proximity)(scaled_capped_business_proximity),
    radius = 5,
    stroke = FALSE, fillOpacity = 0.8,
    label = ~paste0("Business ID: ", business_id, "<br>SCBP: ", round(scaled_capped_business_proximity, 2))
  ) %>%
  addLegend(
    position = "topright",
    pal = colorQuantile("YlOrRd", NULL),
    values = ~scaled_capped_business_proximity,
    title = "Scaled Capped Business Proximity",
    opacity = 1
  ) %>%
  addScaleBar(position = "bottomleft") %>%
  addMiniMap() %>%
  setView(lng = mean(yelp_data_with_population_and_bp$business_long, na.rm = TRUE), 
          lat = mean(yelp_data_with_population_and_bp$business_lat, na.rm = TRUE), 
          zoom = 10)

# Landmarks near restaurants ---------------------------------------

# Helper function for packages
load_or_install <- function(package) {
  if (!require(package, character.only = TRUE)) {
    install.packages(package, dependencies = TRUE)
    library(package, character.only = TRUE)
  }
}

packages <- c("osmdata", "dplyr", "geosphere", "sf", "parallel", "ggplot2", "cowplot", "readr", "leaflet", "dbscan")
lapply(packages, load_or_install)

# Function to query OpenStreetMaps with retry mechanism
query_osm_with_retry <- function(query, retries = 3) {
  result <- NULL
  attempt <- 1
  
  while (is.null(result) && attempt <= retries) {
    try({
      result <- osmdata_sf(query)
    }, silent = TRUE)
    
    if (is.null(result)) {
      Sys.sleep(5)  # Wait for 5 seconds before retrying
      attempt <- attempt + 1
    }
  }
  
  if (is.null(result)) {
    stop("Failed to retrieve data from OSM after ", retries, " attempts.")
  }
  
  return(result)
}

# Define bounding boxes for smaller regions
min_lat <- 39.9
max_lat <- 40.1
min_long <- -75.3
max_long <- -75.0

# Function to create bounding boxes
create_bounding_boxes <- function(min_lat, max_lat, min_long, max_long, n) {
  lat_seq <- seq(min_lat, max_lat, length.out = n + 1)
  long_seq <- seq(min_long, max_long, length.out = n + 1)
  
  bounding_boxes <- list()
  for (i in 1:n) {
    for (j in 1:n) {
      bounding_boxes <- append(bounding_boxes, list(
        c(long_seq[j], lat_seq[i], long_seq[j + 1], lat_seq[i + 1])
      ))
    }
  }
  return(bounding_boxes)
}

# Create bounding boxes (e.g., 4x4 grid)
n <- 4
bounding_boxes <- create_bounding_boxes(min_lat, max_lat, min_long, max_long, n)

# Initialize an empty list to store park data from each bounding box
all_parks <- list()

# Query each bounding box and store the results
for (bbox in bounding_boxes) {
  opq_query <- opq(bbox = bbox) %>%
    add_osm_feature(key = "leisure", value = "park")
  
  parks <- query_osm_with_retry(opq_query)
  all_parks <- c(all_parks, list(parks$osm_points))
}

# Function to ensure consistent columns
ensure_consistent_columns <- function(df, required_columns) {
  for (col in required_columns) {
    if (!(col %in% names(df))) {
      df[[col]] <- NA
    }
  }
  return(df)
}

# Define the required columns based on the first non-empty data frame
first_non_empty <- all_parks[[which(sapply(all_parks, nrow) > 0)[1]]]
required_columns <- names(first_non_empty)

# Ensure all data frames have the same columns
all_parks <- lapply(all_parks, function(df) {
  df <- as.data.frame(df)
  df <- ensure_consistent_columns(df, required_columns)
  df <- df[, required_columns] # Ensure column order is consistent
  return(df)
})

# Combine all parks data into one data frame
all_parks_combined <- do.call(rbind, all_parks)

# Extract the parks' coordinates
park_coordinates <- st_as_sf(all_parks_combined) %>%
  st_coordinates() %>%
  as.data.frame()

names(park_coordinates) <- c("park_long", "park_lat")

# Deduplicate the park coordinates using DBSCAN clustering (otherwise the scores are falsified) 
eps <- 0.001  # Distance threshold for clustering
minPts <- 1   # Minimum points to form a cluster
db <- dbscan(park_coordinates, eps = eps, minPts = minPts)

# Get the center of each cluster
deduplicated_parks <- aggregate(park_coordinates, by = list(cluster = db$cluster), FUN = mean)
deduplicated_parks <- deduplicated_parks[, c("park_long", "park_lat")]

# Function to calculate Haversine distance again 
haversine_distance <- function(lat1, lon1, lat2, lon2) {
  dist <- distHaversine(c(lon1, lat1), c(lon2, lat2))
  return(dist / 1000)  # Convert meters to kilometers
}

# Function to calculate landmark proximity for a chunk of data
calculate_proximity_chunk <- function(chunk, landmark_data, range_km) {
  chunk %>%
    rowwise() %>%
    mutate(
      landmarks_nearby = sum(mapply(function(lat, lon) {
        haversine_distance(business_lat, business_long, lat, lon) <= range_km
      }, landmark_data$park_lat, landmark_data$park_long))
    )
}

# Define the range again (1 km in this case)
range_km <- 1

# Load the dataset with population segments and business proximity
input_file_bp <- "/Users/helua1/Desktop/Master/2. Semester/DSMA/yelp_internal_data_wide_with_population_segments_and_bp.csv"
yelp_data_with_population_and_bp <- read_csv(input_file_bp)

# Detect the number of cores
num_cores <- detectCores() - 1  # Use one less core than the total available

# Split the data into chunks for parallel processing (yay)
chunks <- split(yelp_data_with_population_and_bp, cut(seq(nrow(yelp_data_with_population_and_bp)), num_cores, labels = FALSE))

# Set up parallel processing
cl <- makeCluster(num_cores)
clusterExport(cl, c("chunks", "deduplicated_parks", "haversine_distance", "calculate_proximity_chunk", "range_km"))
clusterEvalQ(cl, library(dplyr))
clusterEvalQ(cl, library(geosphere))

# Run the proximity calculation in parallel
results_list <- parLapply(cl, chunks, function(chunk) {
  calculate_proximity_chunk(chunk, deduplicated_parks, range_km)
})

# Stop the cluster
stopCluster(cl)

# Combine the results from all chunks
yelp_data_with_population_bp_and_lm <- do.call(rbind, results_list)

# Save the updated dataset
write_csv(yelp_data_with_population_bp_and_lm, "/Users/helua1/Desktop/Master/2. Semester/DSMA/yelp_data_with_population_bp_and_lm.csv")


# Visualize the Results --------------------------------

# Histogram of original landmarks_nearby values
p1 <- ggplot(yelp_data_with_population_bp_and_lm, aes(x = landmarks_nearby)) +
  geom_histogram(binwidth = 1, fill = "blue", color = "black") +
  ggtitle("Original Landmarks Nearby") +
  xlab("Landmarks Nearby") +
  ylab("Count")

# Visualize the geographic distribution of landmarks nearby
ggplot(yelp_data_with_population_bp_and_lm, aes(x = business_long, y = business_lat, color = landmarks_nearby)) +
  geom_point() +
  ggtitle("Geographic Distribution of Landmarks Nearby") +
  xlab("Longitude") +
  ylab("Latitude") +
  scale_color_gradient(low = "blue", high = "red")

# Visualize all parks alone
leaflet(data = deduplicated_parks) %>%
  addTiles() %>%
  addCircleMarkers(
    lng = ~park_long,
    lat = ~park_lat,
    radius = 5,
    color = "green",
    stroke = FALSE,
    fillOpacity = 0.7,
    popup = ~paste("Longitude:", park_long, "<br>Latitude:", park_lat)
  ) %>%
  addMiniMap() %>%
  setView(lng = mean(deduplicated_parks$park_long), lat = mean(deduplicated_parks$park_lat), zoom = 12)

# Visualize all parks and businesses with lm score

# Create a color palette based on the number of nearby landmarks
pal <- colorNumeric(palette = "YlOrRd", domain = yelp_data_with_population_bp_and_lm$landmarks_nearby)

# Visualize all parks and restaurants with landmarks_nearby using leaflet
leaflet() %>%
  addTiles() %>%
  addCircleMarkers(
    data = deduplicated_parks,
    lng = ~park_long,
    lat = ~park_lat,
    radius = 5,
    color = "green",
    stroke = FALSE,
    fillOpacity = 0.7,
    popup = ~paste("Longitude:", park_long, "<br>Latitude:", park_lat)
  ) %>%
  addCircleMarkers(
    data = yelp_data_with_population_bp_and_lm,
    lng = ~business_long,
    lat = ~business_lat,
    radius = 5,  # Adjust the radius based on the number of nearby landmarks
    color = ~pal(landmarks_nearby),
    stroke = FALSE,
    fillOpacity = 0.7,
    popup = ~paste("Business ID:", business_id, "<br>Landmarks Nearby:", landmarks_nearby)
  ) %>%
  addLegend(
    position = "bottomright",
    pal = pal,
    values = yelp_data_with_population_bp_and_lm$landmarks_nearby,
    title = "Nearby Landmarks",
    opacity = 1
  ) %>%
  addMiniMap() %>%
  setView(lng = mean(yelp_data_with_population_bp_and_lm$business_long, na.rm = TRUE), 
          lat = mean(yelp_data_with_population_bp_and_lm$business_lat, na.rm = TRUE), 
          zoom = 12)

# Transform the data (Part 2) -------------

library(dplyr)

# Ungroup the dataframe
yelp_data_with_population_bp_and_lm <- yelp_data_with_population_bp_and_lm %>% ungroup()

true_false_none_null_columns <- c(
  "isromantic", "isintimate", "istouristy", "ishipster", 
  "isdivey", "isclassy", "istrendy", "isupscale", "iscasual", 
  "parking_garage", "parking_street", "parking_validated", 
  "parking_lot", "parking_valet", "outdoorseating", 
  "restaurantstableservice", "bikeparking", "happyhour", 
  "byob", "businessacceptscreditcards", "restaurantscounterservice", 
  "hastv", "restaurantsreservations", "restaurantsdelivery", 
  "restaurantstakeout"
)

# Check for missing columns
missing_columns <- setdiff(true_false_none_null_columns, names(yelp_data_with_population_bp_and_lm))

# If there are missing columns, add them with NA values (there were none)
if (length(missing_columns) > 0) {
  cat("Missing columns:", missing_columns, "\n")
  yelp_data_with_population_bp_and_lm[missing_columns] <- NA
}

# Transform the specified columns
yelp_data_with_population_bp_and_lm <- yelp_data_with_population_bp_and_lm %>%
  mutate(across(all_of(true_false_none_null_columns), 
                ~ case_when(
                  . == "False" ~ 0,
                  . == "True" ~ 1,
                  . == "NULL" ~ NA_real_,
                  TRUE ~ NA_real_
                )))

# Transform the 'alcohol' column
yelp_data_with_population_bp_and_lm <- yelp_data_with_population_bp_and_lm %>%
  mutate(alcohol = case_when(
    alcohol %in% c("none", "none'", "'none'") ~ 0,
    alcohol %in% c("beer_and_wine", "beer_and_wine'", "'beer_and_wine'") ~ 1,
    alcohol %in% c("full_bar", "full_bar'", "'full_bar'") ~ 2,
    alcohol == "NULL" ~ NA_real_,
    TRUE ~ NA_real_
  ))

# Transform the 'wifi' column
yelp_data_with_population_bp_and_lm <- yelp_data_with_population_bp_and_lm %>%
  mutate(wifi = case_when(
    wifi %in% c("no", "no'", "'no'", "none") ~ 0,
    wifi %in% c("paid", "paid'", "'paid'") ~ 1,
    wifi %in% c("free", "free'", "'free'") ~ 2,
    wifi == "NULL" ~ NA_real_,
    TRUE ~ NA_real_
  ))

# Transform the 'restaurantspricerange2' column
yelp_data_with_population_bp_and_lm <- yelp_data_with_population_bp_and_lm %>%
  mutate(restaurantspricerange2 = case_when(
    restaurantspricerange2 %in% c("NULL", "None") ~ NA_real_,
    TRUE ~ as.numeric(restaurantspricerange2)
  ))

# Drop the specified columns
columns_to_drop <- c("name", "address", "city", "state", "business_open", "categories", "hours")
yelp_data_with_population_bp_and_lm <- yelp_data_with_population_bp_and_lm %>%
  select(-all_of(columns_to_drop))

# Replace average_stars_2018 with NA if review_count_2018 is 0 (after checking ranges, i have seen that rating is 0, which is not possible by default) 
yelp_data_with_population_bp_and_lm <- yelp_data_with_population_bp_and_lm %>%
  mutate(average_stars_2018 = ifelse(review_count_2018 == 0, NA_real_, average_stars_2018))

# Replace average_stars_2019 with NA if review_count_2019 is 0
yelp_data_with_population_bp_and_lm <- yelp_data_with_population_bp_and_lm %>%
  mutate(average_stars_2019 = ifelse(review_count_2019 == 0, NA_real_, average_stars_2019))

# List of columns to be transformed to integers
count_columns <- c(
  "Total.population_2018", "Total.population_2019",
  "children_2018", "children_2019",
  "youth_2018", "youth_2019",
  "adults_2018", "adults_2019",
  "elders_2018", "elders_2019",
  "surrounding_children_2018", "surrounding_youth_2018", "surrounding_adults_2018", "surrounding_elders_2018",
  "surrounding_children_2019", "surrounding_youth_2019", "surrounding_adults_2019", "surrounding_elders_2019", 
  "check_in_count_2018", "check_in_count_2019", "review_count_2018", "review_count_2019", "attribute_count", "n_photo",
  "overall_review_count"
  
)

rest_columns <- c("alcohol", "wifi", "restaurantspricerange2")

# Transform the specified columns to integers
yelp_data_with_population_bp_and_lm <- yelp_data_with_population_bp_and_lm %>%
  mutate(across(all_of(count_columns), ~ as.integer(.))) %>%
  mutate(across(all_of(true_false_none_null_columns), ~ as.integer(.))) %>%
  mutate(across(all_of(rest_columns), ~ as.integer(.)))

# Verify the transformation
str(yelp_data_with_population_bp_and_lm)

# Verify the transformation
print(head(yelp_data_with_population_bp_and_lm))

# Save the transformed dataset to a new CSV file
write_csv(yelp_data_with_population_bp_and_lm, "/Users/helua1/Desktop/Master/2. Semester/DSMA/yelp_data_transformed_before_MICE.csv")

# Load the dataset back into R
yelp_data_transformed_before_MICE <- read_csv("/Users/helua1/Desktop/Master/2. Semester/DSMA/yelp_data_transformed_before_MICE.csv")

# robust scaling for population variables ------------

library(dplyr)

# List of population variables
population_vars <- c(
  "Total.population_2018", "Total.population_2019",
  "children_2018", "children_2019",
  "youth_2018", "youth_2019",
  "adults_2018", "adults_2019",
  "elders_2018", "elders_2019",
  "surrounding_children_2018", "surrounding_youth_2018", "surrounding_adults_2018", "surrounding_elders_2018",
  "surrounding_children_2019", "surrounding_youth_2019", "surrounding_adults_2019", "surrounding_elders_2019"
)

# Function to create histograms and boxplots
plot_variable_distribution <- function(data, variable) {
  p1 <- ggplot(data, aes(x = .data[[variable]])) +
    geom_histogram(binwidth = 50, fill = "blue", color = "black") +
    ggtitle(paste("Histogram of", variable)) +
    theme_minimal()
  
  p2 <- ggplot(data, aes(y = .data[[variable]])) +
    geom_boxplot(fill = "orange", color = "black") +
    ggtitle(paste("Boxplot of", variable)) +
    theme_minimal()
  
  list(p1, p2)
}

# Save plots to a list
plot_list <- lapply(population_vars, function(var) plot_variable_distribution(yelp_data_transformed_before_MICE, var))

# Display each plot one by one
#for (i in seq_along(plot_list)) {
#  print(plot_list[[i]][[1]])
#  readline(prompt = "Press [enter] to see the next plot (Boxplot)")
#  print(plot_list[[i]][[2]])
#  readline(prompt = "Press [enter] to see the next variable")
#}

# Function to cap values at the 99th percentile
cap_values <- function(x) {
  upper <- quantile(x, 0.99, na.rm = TRUE)
  pmin(x, upper)
}


# Cap and scale the population variables
for (var in population_vars) {
  capped_var <- paste0("capped_", var)
  scaled_var <- paste0("scaled_", capped_var)
  
  # Cap the values
  yelp_data_transformed_before_MICE[[capped_var]] <- cap_values(yelp_data_transformed_before_MICE[[var]])
  
  # Scale the capped values
  yelp_data_transformed_before_MICE[[scaled_var]] <- robust_scaler(yelp_data_transformed_before_MICE[[capped_var]])
}

# Function to create histograms for scaled variables
plot_scaled_variable_histogram <- function(data, variable) {
  ggplot(data, aes(x = .data[[variable]])) +
    geom_histogram(binwidth = 0.1, fill = "blue", color = "black") +
    ggtitle(paste("Histogram of", variable)) +
    theme_minimal()
}

# Create histograms for all scaled variables
for (var in paste0("scaled_capped_", population_vars)) {
  print(plot_scaled_variable_histogram(yelp_data_transformed_before_MICE, var))
  readline(prompt = "Press [enter] to see the next plot")
}

# Save the updated dataset to a new CSV file
write_csv(yelp_data_transformed_before_MICE, "/Users/helua1/Desktop/Master/2. Semester/DSMA/yelp_data_transformed_capped_scaled.csv")






