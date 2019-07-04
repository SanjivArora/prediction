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
  orders <- order(predictors[,date_field], decreasing=TRUE)
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
splitPredictors <- function(predictors, split_type, frac=default_frac, label_days=14, buffer_days=4, date_field="GetDate") {
  
  split_vector <- split_fs[[split_type]](predictors, frac=frac)
  
  train_vector <- split_vector
  test_vector <- !split_vector
  # If splitting on time, drop label_days worth of data at the end of the training set so that no code instances have a response in both test and train sets.
  # Also drop label_days worth of data at the end of the test set so that we have responses available
  if(split_type == "timeSplit") {
    # Drop an additional period to allow for non-exact windows
    total_days_to_drop <- label_days + buffer_days
    predictor_dates <- as.Date(predictors[,date_field])
    train_dates <- predictor_dates[train_vector]
    print(paste("Dropping", total_days_to_drop, "days of training data to prevent code window overlapping with test set"))
    latest <- sort(train_dates, decreasing=TRUE)[[1]]
    latest_eligible <- latest - total_days_to_drop
    train_vector <- unlist(lapply(predictor_dates, function(d) d <= latest_eligible)) & train_vector
    test_dates <- predictor_dates[test_vector]
    print(paste("Dropping", total_days_to_drop, "days of test data to ensure we have responses available"))
    latest <- sort(test_dates, decreasing=TRUE)[[1]]
    latest_eligible <- latest - total_days_to_drop
    test_vector <- unlist(lapply(predictor_dates, function(d) d <= latest_eligible)) & test_vector
  }
  if(sum(train_vector) == 0) {
    stop("No eligible training data")
  }
  if(sum(test_vector) == 0) {
    stop("No eligible test data")
  }
  
  res <- list(train=train_vector, test=test_vector)
  return(res)
}