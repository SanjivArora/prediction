################################################################################
# Cleaning and conditioning
################################################################################

require(magrittr)

# Randomize predictor row and column order to mitigate any algorithmic bias
randomizeOrder <- function(predictors) {
  # Set Seed so that same sample can be reproduced in future
  set.seed(101) 
  
  # Randomize predictor row order
  predictors<-predictors[sample(nrow(predictors)),]
  # Randomize predictor column order
  predictors<-predictors[,sample(ncol(predictors))]
  return(predictors)
}

# Eliminate predictors where Count and PMCount data are out of sync
filterDesynced <- function(predictors, date_fields=c('GetDate', 'ChargeCounterDate')) {
  date_vals <- predictors[,date_fields]
  predictors <- predictors[date_vals[,1] == date_vals[,1],]
  return(predictors)
}

# Eliminate predictors with duplicate GetDate
filterDuplicates <- function(predictors) {
  dups <- duplicated(predictors[,c('Serial', 'GetDate')])
  predictors <- predictors[!dups,]
  return(predictors)
}

# Eliminate predictors with a single unique value
filterSingleValued <- function(predictors) {
  unique_val_counts <- sapply(predictors, function(c) length(unique(c)))
  # Drop predictors with one or more unique values
  predictors <- predictors[,unique_val_counts > 1]
  return(predictors)
}

# Convert replacement dates to relative values
relativeReplacementDates <- function(predictors) {
  replacement_date_col_names <- colnames(predictors)
  replacement_date_cols <- replacement_date_col_names[grep('X.*replacement\\.date', replacement_date_col_names, ignore.case=T)]
  
  to_date <- function(x) {as.Date(as.character(x), '%y%m%d') %>% unlist}
  date_cols <- predictors[,replacement_date_cols]
  date_cols <- apply(date_cols, 2, to_date)
  date_cols <- as.data.frame(date_cols)
  # For some reason R automatically converts the date type to integer
  date_cols <- as.numeric(predictors[,date_field]) - date_cols
  predictors[,replacement_date_cols] <- date_cols
  return(predictors)
}

replaceNA <-function(data){
  temp <- as.data.frame(data)
  # Using an extreme value works well with decision tree methods as it is readily excluded without affecting the normal range
  # Divide by 4 to avoid integer overflow in later processing
  temp[is.na(temp)]<- .Machine$integer.max / 4
  return(temp)
}

# filterInelible is special - it strips serial numbers. Consequently we don't do this in cleanPredictors.
filterIneligible <- function(predictors, string_factors=FALSE) {
  res <- predictors
  char_cols <- unlist(lapply(res, is.character))
  if(string_factors) {
    # Strings to factors
    res[,char_cols] <- lapply(res[,char_cols], as.factor)
    # Exclude Serial
    res <- select(res, -Serial)
  } else {
    res <- res[,!char_cols]
  }
  # Make NA a factor level named None
  factor_cols <- unlist(lapply(res, is.factor))
  res[,factor_cols] <- lapply(res[,factor_cols], function(x) fct_explicit_na(x, na_level="None"))
  # Exclude dates
  res <- res[, !unlist(lapply(res, is.Date))]
  # Replace NAs with default numerical value
  res <- replaceNA(res)
  return(res)
}

# Includes everything but filterIneligible
cleanPredictors <- function(predictors) {
  predictors %>%
    randomizeOrder %>%
    filterDesynced %>%
    filterDuplicates %>%
    filterSingleValued %>%
    relativeReplacementDates
}
