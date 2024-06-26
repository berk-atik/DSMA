# Load necessary libraries
library(dplyr)
library(geosphere)
library(ggplot2)
library(cowplot)
library(readr)

# Load the data
yelp_internal_data <- read_csv("~/dsma_project/sql_9_JSON_2018_2019_w_ac.csv")

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

# Initialize a dataframe to store the results
all_business_proximity_scores <- data.frame()

# Iterate through each postal code to calculate pairwise distances and overlaps
for (postal_code in unique(yelp_internal_data$postal_code)) {
  data_subset <- yelp_internal_data %>%
    filter(postal_code == postal_code) %>%
    mutate(pair_id = row_number())
  
  pairwise_distances <- data_subset %>%
    inner_join(data_subset, by = "postal_code", suffix = c("_i", "_j")) %>%
    filter(pair_id_i < pair_id_j) %>%
    mutate(
      distance = mapply(
        function(lat1, lon1, lat2, lon2) {
          tryCatch({
            haversine_distance(lat1, lon1, lat2, lon2)
          }, error = function(e) {
            NA
          })
        }, business_lat_i, business_long_i, business_lat_j, business_long_j),
      overlap = mapply(calculate_overlap, categories_i, categories_j),
      business_proximity = overlap / distance
    ) %>%
    filter(distance > 0)
  
  business_proximity_scores <- pairwise_distances %>%
    group_by(business_id_i) %>%
    summarise(business_proximity_score = sum(business_proximity, na.rm = TRUE))
  
  all_business_proximity_scores <- bind_rows(all_business_proximity_scores, business_proximity_scores)
}

# Calculate the 99th percentile of business proximity scores
percentile_99th <- quantile(all_business_proximity_scores$business_proximity_score, 0.99)

# Cap the business proximity scores at the 99th percentile
all_business_proximity_scores <- all_business_proximity_scores %>%
  mutate(capped_business_proximity = pmin(business_proximity_score, percentile_99th))

# Function to apply robust scaling
robust_scaler <- function(x) {
  median_x <- median(x)
  iqr_x <- IQR(x)
  if (iqr_x == 0) return(rep(0, length(x)))  # Handle IQR of zero
  scaled_x <- (x - median_x) / iqr_x
  return(scaled_x)
}

# Apply robust scaling to the capped business proximity scores
all_business_proximity_scores <- all_business_proximity_scores %>%
  mutate(scaled_capped_business_proximity = robust_scaler(capped_business_proximity))

# Plot histograms
p1 <- ggplot(all_business_proximity_scores, aes(x = business_proximity_score)) +
  geom_histogram(binwidth = 100, fill = "blue", color = "black") +
  ggtitle("Original Business Proximity Scores") +
  xlab("Business Proximity Score") +
  ylab("Count")

p2 <- ggplot(all_business_proximity_scores, aes(x = capped_business_proximity)) +
  geom_histogram(binwidth = 50, fill = "blue", color = "black") +
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

# Save the data to a CSV file
write_csv(all_business_proximity_scores, "~/dsma_project/yelp_internal_data_with_proximity_scores.csv")

# Display the combined plot
print(combined_plot)

# Optional: Visualize the geographic distribution of scaled capped business proximities
# Assuming latitude and longitude columns exist in yelp_internal_data
ggplot(yelp_internal_data, aes(x = business_long, y = business_lat, color = scaled_capped_business_proximity)) +
  geom_point() +
  ggtitle("Geographic Distribution of Scaled Capped Business Proximities") +
  xlab("Longitude") +
  ylab("Latitude") +
  scale_color_gradient(low = "blue", high = "red")
