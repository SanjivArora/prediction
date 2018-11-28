###############################################################################
# Split predictors
################################################################################

default_frac=0.75


# For reference only, will encourage memorizing cases
randomSplit <- function(predictors, frac=default_frac) {
  indices <- runif(length(predictors)) <= frac
  return(indices)
}

# Approximate split of samples by serial
serialSplit <- function(predictors, frac=default_frac) {
  all_serials <- unique(predictors$Serial)
  serials <- sample(all_serials, size = floor(frac*length(all_serials)), replace = F)
  indices <- predictors$Serial %in% serials
  return(indices)
}

# Approximate split of samples by time, oldest first.
timeSplit <- function(predictors, frac=default_frac) {
  orders <- order(predictors[,date_field])
  newest <- head(orders, floor(length(orders) * (1-frac)))
  res <- rep(TRUE, nrow(predictors))
  res[newest] <- FALSE
  return(res)
}

# Work around Rs inability to compare references to functions
split_fs <- hash(
  c("randomSplit", "serialSplit", "timeSplit"),
  c(randomSplit, serialSplit, timeSplit)
)

# Provide vectors for selecting train and test sets from predictors. Return a list with named 'test' and 'train' logical vectors.
splitPredictors <- function(predictors, split_type, frac=default_frac, sc_code_days=14, date_field="GetDate") {
  
  split_vector <- split_fs[[split_type]](predictors)
  
  train_vector <- split_vector
  test_vector <- !split_vector
  # If splitting on time, drop sc_code_days worth of data before split so we don't count cases where there is overlap.
  if(split_type == "timeSplit") {
    # Drop an additional period to allow for non-exact windows
    total_days_to_drop <- sc_code_days + 4
    print(paste("Dropping", total_days_to_drop, "days of training data to prevent SC code window overlapping with test set"))
    predictor_dates <- as.Date(predictors[,date_field])
    train_dates <- predictor_dates[train_vector]
    latest <- sort(train_dates, decreasing=TRUE)[[1]]
    latest_eligible <- latest - total_days_to_drop
    train_vector <- unlist(lapply(predictor_dates, function(d) d <= latest_eligible)) & train_vector
    if(sum(train_vector) == 0) {
      stop("No eligible training data")
    }
  }
  
  res <- list(train=train_vector, test=test_vector)
  return(res)
}