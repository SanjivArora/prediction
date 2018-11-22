require(dplyr)
require(memoise)
require(mlr)
require(itertools)
require(forcats)


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
debugSource("lib/Results.R")


#sources=c('Count', 'PMCount', 'Jam')
sources=c('PMCount', 'Count') 
#sources=c('PMCount') 


library(profvis)
#profvis({

# Number of days of predictor data files to use for training
data_days <- 1000

# If true, aim to use every observation (overrides total_samples)
all_data <- TRUE
# If true, sample positive cases to value of positive_samples rather than at base rate
upsample_positive <- TRUE
# Ignore upsample_positives if all_data is true - not required as we have training weights
if(all_data) {
  upsample_positive <- FALSE
}
# If true, take a maximum of one sample for each observation when upsampling is enabled
cap_sampling = TRUE
# Target samples (will pick up extra samples where there are multiple applicable codes)
total_samples <- 50000

# Maximum number of days to predict SC code
sc_code_days <- 14
#sc_code_days=2

# Minimum number of sample-days to predict a label
min_count <- 300

# Offsets to use for generating deltas for numerical data
delta_days <- c(14)
#delta_days = c(1, 2)



deltas <- FALSE

# Drop non-delta numerical values
only_deltas <- FALSE

regions = c(
  'RNZ'
)

models= c(
  'E15',
  'E16',
  'E17',
  'E18',
  'E19'
  # Exclude G models for now as counter names and SC subcodes differ 
  #'G69',
  #'G70'
  # TODO: check with Karl whether these are equivalent to E17 and E19 per Rotem's data
  #'G71',
  #'G73'
)

parallel=TRUE
#parallel=FALSE


# If upsampling is enabled, SC code instances will constitute at least this fraction of samples
positive_sample_fraction <- 0.5


# Specify target codes. Note: prediction is based on a <code>_<subcode> label, currently we don't filter on subcode.
target_codes <- list(
  200:299, # "C01 NOT FUNCTION AT ALL"
  600:699, # "C01 NOT FUNCTION AT ALL"
  800:899, # "C01 NOT FUNCTION AT ALL
  900:999 # "C01 NOT FUNCTION AT ALL"  
)
target_codes <- Reduce(append, target_codes)

# Exclude 899
#target_codes <- target_codes[target_codes != 899]

target_code_hash <- hash()
for(c in target_codes) {
  target_code_hash[[as.character(c)]] <- TRUE
}

date_field <- "FileDate"

feature_files <- c('top.txt')
selected_features <- TRUE

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
code_indices <- plapply(splitDataFrame(codes_all), function(c) has.key(c$SC_CD, target_code_hash))
codes <- codes_all[unlist(code_indices),]

################################################################################
# Sample dataset
################################################################################

all_data_files <- instancesForDir()

# Restrict data set to days for which we have current SC code data, and a maximum of data_days
sc_files <- filterBy(all_data_files, function(f) f$source=="SC")
sc_files <- sortBy(sc_files, function(f) f$date)
stopifnot(length(sc_files) > 0)
latest_sc_file_date <- as.Date(last(sortBy(sc_files, function(f) f$date))$date)
latest_data_file_date <- latest_sc_file_date - sc_code_days
filtered_data_files <- filterBy(all_data_files, function(f) as.Date(f$date) < latest_data_file_date)
file_sets <- getDailyFileSets(filtered_data_files, sources)
data_files <- unlist(file_sets[1:data_days])

require(profvis)
#profvis({

counts <- dataFilesToCounter(data_files, sources, codes, sc_code_days, min_count, cap_sampling, features=fs, parallel=parallel)
print(counts$getEligibleCounts())
if(all_data) {
  counts$setTargetTotal(counts$getTotal())
} else {
  counts$setTargetTotal(total_samples)
}
counts$setTargetPositiveFraction(positive_sample_fraction)
counts$setUpsamplePositive(upsample_positive)

predictors_all <- dataFilesToDataset(
  data_files,
  sources,
  codes,
  counts,
  sc_code_days,
  delta_days=delta_days,
  deltas=deltas,
  only_deltas=only_deltas,
  features=fs,
  parallel=parallel
)
#View(predictors[1:5,1:5])
#})

################################################################################
# Eliminate predictors with a single unique value
################################################################################

unique_val_counts <- sapply(predictors_all, function(c) length(unique(c)))
# Drop predictors with one or more unique values
predictors <- predictors_all[,unique_val_counts > 1]

###############################################################################
# Get labels for codes that meet the required threshold for instances
################################################################################

used_labels <- keys(counts$getEligibleCounts())
used_labels <- used_labels[used_labels!="0"]

################################################################################
# Add predictors for historical SC codes
################################################################################

serial_to_codes <- counts$getSerialToEligibleCodes()

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

################################################################################
# Train and test datasets
################################################################################
# Select 75% of data for training set, 25% for test set

# Set Seed so that same sample can be reproduced in future
set.seed(101) 

# Randomize predictor row order
predictors<-predictors[sample(nrow(predictors)),]
# Randomize predictor column order
predictors<-predictors[,sample(ncol(predictors))]


getMatchingCodesForIndex <- function(i) {
  row <- predictors[i, c("Serial", date_field)]
  getMatchingCodes(serial_to_codes[[row$Serial]], row[,date_field], counts$getSCDays())
}

# Pass in only required values for efficiency
# TODO: only evaluate for serials already in serial_to_codes
matching_code_sets <- plapply(
  1:nrow(predictors),
  getMatchingCodesForIndex
)

# Get the first instance of each SC code (works due to uniqueBy returning the last matching value)
matching_code_sets_sorted <- lapply(
  matching_code_sets,
  function(cs) sortBy(cs, function(c) c$OCCUR_DATE, desc=TRUE)
)
matching_code_sets_unique <- lapply(matching_code_sets_sorted, function(cs) uniqueBy(cs, function(c) codeToLabel(c)))

# Summarize code counts for final sampling
# final_codes <- lapply(unlist(matching_code_sets_unique, recursive=FALSE), function(c) codeToLabel(c))
# final_counts <- sort(table(unlist(final_codes)), decreasing=TRUE)
# final_control_count <- length(filterBy(matching_code_sets_unique, function(cs) length(cs)==0))

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
colnames(responses) <- used_labels


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



default_frac <- .75 
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
  orders <- order(predictors$FileDate)
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
  print(paste("Dropping", sc_code_days, "days of training data to prevent SC code window overlapping with test set"))
  predictor_dates <- as.Date(predictors$FileDate)
  train_dates <- predictor_dates[train_vector]
  latest <- sort(train_dates, decreasing=TRUE)[[1]]
  latest_eligible <- latest - sc_code_days
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

take_eligible <- function(dataset, string_factors=TRUE) {
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

importance <- TRUE

data <- bind_cols(predictors_eligible, responses)

# Make R-standard names
label_names <- make.names(used_labels)
predictor_names <- make.names(colnames(data))

colnames(data) <- predictor_names

train_data <- data[train_vector,]
test_data <- data[test_vector,]

train_task <- makeMultilabelTask(data = train_data, target = unlist(label_names))
test_task <- makeMultilabelTask(data = test_data, target = unlist(label_names))

# Calculate weights such that each positive class has approximately .5/n_positive_classes, with the remaining half allocated to control cases.
# Calculate target counts for each class

train_target <- getTaskTargets(train_task)
train_counts <- responsesToCounts(train_target)
n_positive <- sum(responsesToPositiveCounts(train_target))
positive_class_share <- 1 / ncol(responses)
control_class_share <- 1

n_train_samples <- nrow(train_data)

weightForIndex <- function(i) {
  row <- train_target[i,]
  # Default to control weight
  weight <- control_class_share / train_counts[["0"]]
  # Take the maximum class weight if a class is positive
  if(any(unlist(row))) {
    for(k in names(train_counts)) {
      if(k!="0" && row[,make.names(k)]) {
        weight <- max(weight, positive_class_share / train_counts[[k]])
      }
    }
  }
  return(weight)
}
  
weights <- plapply(1:nrow(train_target), weightForIndex)
weights <- unlist(weights)

lrn <- makeLearner("classif.ranger", par.vals=list(
  num.threads = detectCores() - 1,
  num.trees = 2000,
  importance = 'impurity'
  #sample.fraction = 0.2
))

lrn <- makeMultilabelBinaryRelevanceWrapper(lrn)
lrn <- setPredictType(lrn, "prob")
#lrn <- setPredictType(lrn, "response")


mod <- mlr::train(lrn, train_task, weights=weights)
#mod <- mlr::train(lrn, train_task)
pred <- predict(mod, test_task)


################################################################################
# Evaluate performance
################################################################################

mlr::performance(pred, measure <- list(multilabel.hamloss, multilabel.subset01, multilabel.f1))
perf <- getMultilabelBinaryPerformances(pred, measures <- list(mmce, auc))
sorted_perf <- perf[order(perf[,"auc.test.mean"], decreasing=TRUE),]

print(summary(pred$data))

print(counts$getFrequencies())

print(sorted_perf)

extra_stats <- getExtraMultiLabelStats(pred, used_labels, counts)
print(extra_stats)

showModelFeatureImportance <- function(model, n=25) {
  x <- getFeatureImportance(model)
  features_desc <- x$res[, order(x$res[1,], decreasing=T)]
  top_n <- t(features_desc[,1:n])
  print(top_n)
}

if(importance) {
  for(label in label_names) {
    cat("\n\n")
    print(paste("Importance for", label))
    showModelFeatureImportance(mod$learner.model$next.model[[label]])
  }
}

predicted <- pred$data[,grepl("^prob.", names(pred$data))]
response <- pred$data[,grepl("^truth", names(pred$data))]
p <- as.matrix(sapply(predicted, as.numeric))
r <- as.matrix(sapply(response, as.numeric))

# We can only graph classes with positive labels
to_graph <- apply(r, 2, function(c) sum(c) > 0)

plot_roc <- function(prob, truth) {
  roc_pred <- ROCR::prediction(prob, truth)
  roc_perf <- ROCR::performance(roc_pred, 'tpr', 'fpr')
  ROCR::performance(roc_pred, 'auc')
  ROCR::plot(roc_perf, colorize=TRUE)
}

plot_prec <- function(prob, truth) {
  roc_pred <- ROCR::prediction(prob, truth)
  roc_perf <- ROCR::performance(roc_pred, 'prec', 'rec')
  ROCR::performance(roc_pred, 'auc')
  ROCR::plot(roc_perf, colorize=TRUE)
}

# Plot ROC and precision for all submodels
#plot_roc(p[,to_graph], r[,to_graph])

# Plot precision vs recall for allsubmodels
plot_prec(p[,to_graph], r[,to_graph])


################################################################################
# Save top features
################################################################################

# top_features <- topMultilabelModelFeatures(mod)
# writeFeatures(row.names(top_features), "top.txt")