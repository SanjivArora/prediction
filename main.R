require(dplyr)
require(memoise)
require(mlr)
require(itertools)
require(forcats)
require(magrittr)

# MemoiseCache must be loaded first
debugSource("lib/Parallel.R")
debugSource("lib/Util.R")
debugSource("lib/Dates.R")
debugSource("lib/MemoiseCache.R")
debugSource("lib/Visualization.R")
debugSource("lib/Feature.R")
debugSource("lib/FeatureSelection.R")
debugSource("lib/Feature.R")
debugSource("lib/SCFile.R")
debugSource("lib/Sampling.R")
debugSource("lib/Logging.R")
debugSource("lib/DataFile.R")
debugSource("lib/Model.R")
debugSource("lib/Results.R")
debugSource("lib/Evaluate.R")

#sources=c('Count', 'PMCount', 'Jam')
sources=c('PMCount', 'Count') 
#sources=c('PMCount') 

feature_files <- c('top.txt')
selected_features <- FALSE

library(profvis)
#profvis({

# Number of days of predictor data files to use for training
data_days <- 1000

# Fraction of observations to use
sample_rate <- 0.3

# Build models for up to this many <SC>_<subcode> pairs
max_models <- 15

# Maximum number of days to predict SC code
sc_code_days <- 14
#sc_code_days=2

# Offsets to use for generating deltas for numerical data
delta_days <- c(14)
#delta_days = c(1, 2)

# Fraction of days to use for training (less SC overlap window)
training_frac = 0.9

historical_sc_predictors <- TRUE
deltas <- FALSE

# Drop non-delta numerical values
only_deltas <- FALSE

regions = c(
  'RNZ'
)

models= c(
  'E15',
  'E16'
  # 'E17',
  # 'E18',
  # 'E19'
  # Exclude G models for now as counter names and SC subcodes differ 
  #'G69',
  #'G70'
  # TODO: check with Karl whether these are equivalent to E17 and E19 per Rotem's data
  #'G71',
  #'G73'
)

parallel=TRUE
#parallel=FALSE

# Specify target codes. Note: prediction is based on a <code>_<subcode> label, currently we don't filter on subcode.
# target_codes <- list(
#   200:299, # "C01 NOT FUNCTION AT ALL"
#   600:699, # "C01 NOT FUNCTION AT ALL"
#   800:899, # "C01 NOT FUNCTION AT ALL
#   900:999 # "C01 NOT FUNCTION AT ALL"  
# )
target_codes <- list(1:999)
target_codes <- Reduce(append, target_codes)

# Exclude 899
#target_codes <- target_codes[target_codes != 899]

target_code_hash <- hash()
for(c in target_codes) {
  target_code_hash[[as.character(c)]] <- TRUE
}

date_fields <- c('GetDate', 'ChargeCounterDate')
date_field <- date_fields[[1]]

# Wait an additional period for SC code data as up to date information for a machine, and the typical lag seems to be a few days.
sc_data_buffer = 4

# Number of trees to use for random forest
#ntree = 1000
ntree = 500

relative_replacement_dates <- TRUE

################################################################################
# Feature Names
################################################################################

if(selected_features) {
  fs <- readFeatures(feature_files)
} else {
  fs <- FALSE
}

################################################################################
# SC Codes
################################################################################

codes_all <- codesForRegionsAndModels(regions, models, parallel)
# Filter SC codes to target codes
code_dups <- duplicated(codes_all[,c('Serial', 'SC_CD', 'SC_SERIAL_CD', 'OCCUR_DATE')])
codes_unique <- codes_all[!code_dups,]
code_indices <- plapply(splitDataFrame(codes_unique), function(c) has.key(c$SC_CD, target_code_hash))
codes <- codes_unique[unlist(code_indices),]

################################################################################
# Sample dataset
################################################################################

all_data_files <- instancesForDir()

# Restrict data set to days for which we have current SC code data, and a maximum of data_days
sc_files <- filterBy(all_data_files, function(f) f$source=="SC")
sc_files <- sortBy(sc_files, function(f) f$date)
stopifnot(length(sc_files) > 0)
latest_sc_file_date <- as.Date(last(sortBy(sc_files, function(f) f$date))$date)
latest_data_file_date <- latest_sc_file_date - sc_code_days - sc_data_buffer
filtered_data_files <- filterBy(
  all_data_files,
  function(f) {
    as.Date(f$date) < latest_data_file_date &&
      f$model %in% models
  }
)
file_sets <- getDailyFileSets(filtered_data_files, sources)
data_files <- unlist(file_sets[1:data_days])

require(profvis)
#profvis({

predictors_all <- dataFilesToDataset(
  data_files,
  sources,
  codes,
  sample_rate,
  sc_code_days,
  delta_days=delta_days,
  deltas=deltas,
  only_deltas=only_deltas,
  features=fs,
  parallel=parallel
)
predictors <- predictors_all


################################################################################
# Randomize predictor row and column order to mitigate any algorithmic bias
################################################################################

# Set Seed so that same sample can be reproduced in future
set.seed(101) 

# Randomize predictor row order
predictors<-predictors[sample(nrow(predictors)),]
# Randomize predictor column order
predictors<-predictors[,sample(ncol(predictors))]

################################################################################
# Eliminate predictors where Count and PMCount data are out of sync
################################################################################

date_vals <- predictors[,date_fields]
predictors <- predictors[date_vals[,date_fields[[1]]] == date_vals[,date_fields[[2]]],]

################################################################################
# Eliminate predictors with duplicate GetDate
################################################################################

dups <- duplicated(predictors[,c('Serial', 'GetDate')])
predictors <- predictors[!dups,]

################################################################################
# Eliminate predictors with a single unique value
################################################################################

unique_val_counts <- sapply(predictors, function(c) length(unique(c)))
# Drop predictors with one or more unique values
predictors <- predictors[,unique_val_counts > 1]

################################################################################
# Convert replacement dates to relative values
################################################################################

if(relative_replacement_dates) {
  replacement_date_col_names <- colnames(predictors)
  replacement_date_cols <- replacement_date_col_names[grep('X.*replacement\\.date', replacement_date_col_names, ignore.case=T)]
  
  to_date <- function(x) {as.Date(as.character(x), '%y%m%d') %>% unlist}
  date_cols <- predictors[,replacement_date_cols]
  date_cols <- apply(date_cols, 2, to_date)
  date_cols <- as.data.frame(date_cols)
  # For some reason R automatically converts the date type to integer
  date_cols <- as.numeric(predictors[,date_field]) - date_cols
  predictors[,replacement_date_cols] <- date_cols
}

###############################################################################
# Build list of matching code sets for each row
################################################################################

serial_to_codes <- makeSerialToCodes(codes)

getMatchingCodesForIndex <- function(i) {
  row <- predictors[i, c("Serial", date_field)]
  getMatchingCodes(
    getWithDefault(serial_to_codes, row$Serial, list()),
    row[,date_field],
    sc_code_days
  )
}

# Pass in only required values for efficiency
matching_code_sets <- plapply(
  1:nrow(predictors),
  getMatchingCodesForIndex,
  parallel=parallel
)

# Get the first instance of each SC code (works due to uniqueBy returning the last matching value)
# Indexing by time would be ideal but probably not worth the added complexity.
matching_code_sets_sorted <- plapply(
  matching_code_sets,
  function(cs) sortBy(cs, function(c) c$OCCUR_DATE, desc=TRUE),
  parallel=parallel
)

matching_code_sets_unique <- plapply(
  matching_code_sets_sorted,
  function(cs) uniqueBy(cs, function(c) codeToLabel(c)),
  parallel=parallel
)


###############################################################################
# Build list of matching code sets for each row
################################################################################

label_to_row_indices <- hash()
for(i in 1:nrow(predictors)) {
  cs <- matching_code_sets_unique[[i]]
  for (c in cs) {
    label <- codeToLabel(c)
    indices <- getWithDefault(label_to_row_indices, label, list())
    label_to_row_indices[[label]] <- append(indices, i)
  }
}

label_counts <- mapHash(label_to_row_indices, function(indices) length(indices))

label_to_unique_serials <- mapHash(
  label_to_row_indices,
  function(row_indices) {
    unique(predictors$Serial[unlist(row_indices)])
  }
)

###############################################################################
# Get labels for codes that meet the required threshold for instances
################################################################################

label_to_serial_count <- mapHash(label_to_unique_serials, function(serials) length(serials))

label_to_count_and_unqiues <- mapHashWithKeys(
  label_counts,
  function(label, count) c(count, label_to_serial_count[[label]])
)

# Use geometric mean of observation count and number of unqiue serials as a figure of merit
label_to_priority <- mapHash(label_to_count_and_unqiues, geomMean)
#label_to_priority <- label_to_serial_count

top_labels <- tail(sortBy(label_to_priority, function(x) x[[1]]), max_models)
used_labels <- names(top_labels)

################################################################################
# Add predictors for historical SC codes
################################################################################

if(historical_sc_predictors) {
  # Get the last instance of each SC code (works due to uniqueBy returning the last matching value)
  getPreviousCodesForRow <- function(row) {
    cs <- getMatchingCodesBefore(serial_to_codes[[row$Serial]], row[,date_field])
    sorted <- sortBy(cs, function(c) c$OCCUR_DATE)
    res <- uniqueBy(cs, function(c) codeToLabel(c))
    return(res)
  }
  
  # Pass in only required values for efficiency
  getPreviousCodesForIndex <- function(i) {
    getPreviousCodesForRow(predictors[i,c("Serial", date_field)])
  }
  
  previous_code_sets_unique <- plapply(
    1:nrow(predictors),
    getPreviousCodesForIndex,
    parallel=parallel
  )
  
  index_to_hist_sc <- function(i, default_delta=10000) {
    code_set <- previous_code_sets_unique[[i]]
    cs <- groupBy(code_set, function(c) codeToLabel(c))
    part <- list()
    predictor_date <- predictors[i, date_field]
    for(label in used_labels) {
      if(has.key(label, cs)) {
        c <- cs[[label]][[1]]
        delta <- predictor_date - c$OCCUR_DATE
      } else {
        delta <- default_delta
      }
      part <- append(part, as.numeric(delta))
    }
    res <- matrix(unlist(part), nrow=1)
    res <- as.data.frame(res)
    return(res)
  }
  
  hist_sc_predictors_parts <- plapply(1:nrow(predictors), index_to_hist_sc, parallel=parallel)
  hist_sc_predictors <- bindRowsForgiving(hist_sc_predictors_parts)
  colnames(hist_sc_predictors) <- paste("days.since.last", used_labels, sep=".")
  
  predictors <- cbind(predictors, hist_sc_predictors)
}

################################################################################
# Train and test datasets
################################################################################

# Calculate responses
code_set_to_labels <- function(code_set) {
  cs <- lapply(code_set, function(c) codeToLabel(c))
  part <- used_labels %in% cs
  res <- matrix(part, nrow=1)
  res <- as.data.frame(res)
  return(res)
}

responses_parts <- plapply(matching_code_sets_unique, code_set_to_labels, parallel=parallel)
responses <-bindRowsForgiving(responses_parts)
# Make R-standard names
label_names <- make.names(used_labels)
colnames(responses) <- label_names

# Stats for dataset
print(paste(nrow(predictors), "total observations"))

responsesToPositiveCounts <- function(responses) {sapply(responses, sum)}
responsesToControlCount <- function(responses) {sum(apply(responses, 1, Negate(any)))}
responsesToCounts <- function(responses) {
  counts <- responsesToPositiveCounts(responses)
  counts[["0"]] <- responsesToControlCount(responses)
  return(counts)
}

print("Observation counts for SC codes:")
print(responsesToCounts(responses))

default_frac <- training_frac
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

#split_type <- "randomSplit"
#split_type <- "serialSplit"
split_type <- "timeSplit"

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

################################################################################
# Restrict to valid numeric values
################################################################################

replace_na<-function(data){
  temp <- as.data.frame(data)
  # Using an extreme value works well with decision tree methods as it is readily excluded without affecting the normal range
  # Divide by 4 to avoid integer overflow in later processing
  temp[is.na(temp)]<- .Machine$integer.max / 4
  return(temp)
}

take_eligible <- function(dataset, string_factors=FALSE) {
  res <- dataset
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
  res <- replace_na(res)
  return(res)
}

predictors_eligible <- take_eligible(predictors, string_factors=FALSE)


################################################################################
# Model
################################################################################

require(mlr)


train_data <- predictors_eligible[train_vector,]
train_responses <- responses[train_vector,]

# Train models in parallel as despite native threading support there are substantial serial sections
models <- trainModelSet(label_names, train_data, train_responses, parallel=parallel)

test_data <- predictors_eligible[test_vector,]
test_responses <- responses[test_vector,]

evaluateModelSet(models, test_data, test_responses)

################################################################################
# Predict with first partial test set
################################################################################
# 
# split_vector1 <- split_fs[["timeSplit"]](predictors, frac=.925)
# split_vector2 <- split_fs[["timeSplit"]](predictors, frac=.95)
# test_data1 <- data[!split_vector & split_vector1,]
# test_data1a <- data[!split_vector & !split_vector1 & split_vector2,]
# test_data2 <- data[!split_vector & !split_vector1,]


################################################################################
# Evaluate performance
################################################################################

# extra_stats <- getExtraMultiLabelStats(pred, used_labels, counts)
# print(extra_stats)

# 
# if(importance) {
#   for(label in label_names) {
#     cat("\n\n")
#     print(paste("Importance for", label))
#     showModelFeatureImportance(mod$learner.model$next.model[[label]])
#   }
# }

################################################################################
# Save top features
################################################################################

# top_features <- topMultilabelModelFeatures(mod, frac=0.4)
# writeFeatures(row.names(top_features), "top.txt")