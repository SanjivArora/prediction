################################################################################
# Cleaning and conditioning
################################################################################

require(magrittr)

clean_log <- getModuleLogger("Model")

# Randomize predictor row and column order to mitigate any algorithmic bias
randomizeOrder <- function(predictors) {
  clean_log$debug("Randomizing predictor order")
  # Set Seed so that same sample can be reproduced in future
  set.seed(101) 
  
  # Randomize predictor row order
  predictors<-predictors[sample(nrow(predictors)),]
  # Randomize predictor column order
  predictors<-predictors[,sample(ncol(predictors))]
  return(predictors)
}

# Eliminate predictors where Count and PMCount data are out of sync
filterDesynced <- function(preds, date_fields=c('GetDate', 'ChargeCounterDate')) {
  clean_log$debug("Filtering desynchronized observations")
  date_vals <- preds[,date_fields]
  deltas <- date_vals[,1] - date_vals[,2]
  preds <- preds[abs(deltas) <= 1,]
  return(preds)
}

# Eliminate predictors with duplicate GetDate
filterDuplicates <- function(predictors) {
  clean_log$debug("Filtering duplicate observations")
  dups <- duplicated(predictors[,c('Serial', 'GetDate')])
  predictors <- predictors[!dups,]
  return(predictors)
}

# Eliminate predictors with a single unique value
# TODO: eliminate predictors with a single unique value per model
filterSingleValued <- function(predictors) {
  clean_log$debug("Filtering single valued observations")
  unique_val_counts <- sapply(predictors, function(c) length(unique(c)))
  # Drop predictors with one or more unique values
  predictors <- predictors[,unique_val_counts > 1]
  return(predictors)
}

# Standard format for recognized dates
standardizeDates <- function(predictors) {
  clean_log$debug("Formatting dates")
  col_names <- colnames(predictors)
  cols <- col_names[grep('.*replacement\\.date', col_names, ignore.case=T)]
  
  if(length(cols)==0) {
    return(predictors)
  }
  
  to_date <- function(x) {as.Date(as.character(x), '%y%m%d') %>% unlist}
  date_cols <- predictors[,cols]
  date_cols <- colwise(to_date)(date_cols)
  date_cols <- as.data.frame(date_cols)
  predictors[,cols] <- date_cols
  return(predictors)
}

# Convert replacement dates to relative values and add these as new columns ending with ".replaced"
relativeReplacementDates <- function(predictors) {
  clean_log$debug("Adding relative replacement dates")
  replacement_date_cols <- colnames(predictors)[grep('X.*replacement\\.date\\..*\\.read\\.only$', colnames(predictors), ignore.case=T)]
  replacement_date_cols_new <- paste(replacement_date_cols, '.relative', sep='')
  
  if(length(replacement_date_cols)==0) {
    return(predictors)
  }
  
  for (i in 1:length(replacement_date_cols)) {
    predictors[,replacement_date_cols_new[[i]]] <- (predictors$RetrievedDate - predictors[,replacement_date_cols[[i]]]) %>% as.numeric
  }
  return(predictors)
}

replaceNA <-function(data){
  clean_log$debug("Replacing NAs")
  temp <- as.data.frame(data)
  # Using an extreme value works well with decision tree methods as it is readily excluded without affecting the normal range
  # Divide by 4 to avoid integer overflow in later processing
  temp[is.na(temp)]<- .Machine$integer.max / 4
  return(temp)
}

replaceLogical <- function(data) {
  data %<>% mutate_each(funs(if(is.logical(.)) as.numeric(.) else .))
  return(data)
}

# filterIneligibleFields is special - it strips serial numbers. Consequently we don't do this in cleanPredictors.
# If string_factors is true, convert all strings to factors and include them in the result.
filterIneligibleFields <- function(predictors, string_factors=c("Model"), exclude_cols=c('Serial'), exclude_dates=TRUE, replace_na=TRUE, replace_logical=TRUE) {
  clean_log$debug("Filtering ineligible fields")
  # Convert characters to factors if so specified
  char_cols <- unlist(lapply(predictors, is.character))
  string_factors <- intersect(names(predictors), string_factors)
  factors <- lapply(predictors[string_factors], as.factor)
  # Make NA a factor level named None
  factors <- lapply(factors, function(x) fct_explicit_na(x, na_level="None"))
  res <- predictors[,!char_cols]
  res[,string_factors] <- factors
  
  # Exclude nominated columns
  res <- res[,!colnames(res) %in% exclude_cols]

  if(exclude_dates) {
    res <- res[, !unlist(lapply(res, is.Date))]
  }
  
  # Replace NAs with default numerical value
  if(replace_na) {
    res %<>% replaceNA
  }
  
  if(replace_logical) {
    res %<>% replaceLogical
  }
  return(res)
}

verToInt <- function(ver) {
  # Remove characters that aren't numeric or separators
  x <- gsub('[^0-9:.]', '', ver, perl=TRUE)
  parts <- strsplit(x, '[.:]')[[1]]
  parts <- lapply(parts, function(x) sub('', '0', x))
  parts <- lapply(parts, as.integer)
  # If there are fewer than three parts in the firmrware version, append zeroes
  if(length(parts) < 3) {
    parts %<>% append(rep(0, 3-length(parts)))
  }
  # Zero-pad components to three digits
  parts <- lapply(parts, function(x) formatC(x, width = 3, format = "d", flag = "0"))
  # Interpret as integer
  s <- paste(parts, collapse='', sep='')
  res <- as.integer(s)
  return(res)
}

# Convert version string to integer, attempting as far as possible to preserve ordering
# Supports a maximum of three segments in the version string as we zero-pad to three digits and the maximum integer is 10 digit
processRomVer <- function(predictors) {
  clean_log$debug("Processing RomVer values")
  # Take semi-numeric firmware versions
  ver_idxs <- grepl('RomVer_VER_.*', names(predictors))
  ver_idxs <- which(ver_idxs)
  ver_names <- names(predictors)[ver_idxs]
  ver_orig <- predictors[,ver_idxs]
  vers <- plapply(ver_idxs, function(i) lapply(predictors[,i], verToInt) %>% unlist)
  res <- predictors[!grepl('RomVer_.*', names(predictors))]
  res[ver_names] <- vers
  res[,paste(ver_names, '.raw', sep='')] <- ver_orig
  return(res)
}

# Includes everything but filterIneligibleFileds
cleanPredictors <- function(predictors, randomize_order=randomize_predictor_order, relative_replacement_dates=TRUE) {
  if(randomize_order) {
    predictors %<>% randomizeOrder
  } else {
    predictors %<>% arrange(RetrievedDateTime)
  }
  predictors %<>% filterDesynced
  predictors %<>% filterDuplicates
  predictors %<>% standardizeDates
  if(relative_replacement_dates) {
    predictors %<>% relativeReplacementDates
  }
  predictors %<>% processRomVer
}
