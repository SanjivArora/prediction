require(dplyr)
require(memoise)
require(doParallel)
require(parallel)
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
debugSource("lib/FeatureNames.R")
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

# Target samples (will pick up extra samples where there are multiple applicable codes)
total_samples <- 10000

# Maximum number of days to predict SC code
sc_code_days <- 14
#sc_code_days=2

# Minimum number of sample-days to predict an SC code
min_count <- 100

# Offsets to use for generating deltas for numerical data
delta_days <- c(1, 3, 7)
#delta_days = c(1, 2)

deltas <- TRUE

# Drop non-delta numerical values
only_deltas <- FALSE

regions = c(
  'RNZ'
)

models= c(
  'E16',
  'E15'
)

parallel=TRUE
#parallel=FALSE

features = c(
  #'test5.txt'
)

daily_features = c(
  #'pcu_daily.txt',
  #'jobs_daily.txt'
  #'shortlist_daily.txt'
)


# Total samples for SC code instances
positive_samples <- total_samples / 2
# Total sample for control instances
control_samples <- total_samples / 2


target_codes <- list(
  200:299, # "C01 NOT FUNCTION AT ALL"
  600:699, # "C01 NOT FUNCTION AT ALL"
  800:899, # "C01 NOT FUNCTION AT ALL
  900:999 # "C01 NOT FUNCTION AT ALL"  
)
target_codes <- Reduce(append, target_codes)

# Exclude 899
target_codes <- target_codes[target_codes != 899]

target_code_hash <- hash()
for(c in target_codes) {
  target_code_hash[[as.character(c)]] <- TRUE
}


################################################################################
# Feature Names
################################################################################

# fs <- append(
#   read_features(features),
#   read_daily_features(daily_features)
# )

# TODO: Investigate apparent effect of feature order on RandomTree implementation
#fs<-sample(fs)


################################################################################
# SC Codes
################################################################################

codes_all <- codesForRegionsAndModels(regions, models, parallel)
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

counts <- dataFilesToCounter(data_files, sources, codes, sc_code_days, min_count, parallel=parallel)
print(counts$getCounts())
counts$setTargetSC(positive_samples)
counts$setTargetControl(control_samples)
predictors <- dataFilesToDataset(
  data_files,
  sources,
  codes,
  counts,
  sc_code_days,
  delta_days=delta_days,
  deltas=deltas,
  only_deltas=only_deltas,
  parallel=parallel
)
#View(predictors[1:5,1:5])
#})

#predictors <- predictors %>% mutate_if(sapply(predictors, is.factor), as.character)


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

serial_to_codes <- counts$getSerialToCodes()

date_field <- "GetDate"
getMatchingCodesForRow <- function(row) {
  getMatchingCodes(serial_to_codes[[row$Serial]], row[,date_field], counts$getSCDays())
}

# Pass in only required values for efficiency)
matching_code_sets <- lapply(
  splitDataFrame(predictors[,c("Serial", date_field)]),
  getMatchingCodesForRow
)

# Summarize code counts for final sampling
matching_code_sets_unique <- lapply(matching_code_sets, function(cs) uniqueBy(cs, function(c) c$SC_CD))
final_codes <- lapply(unlist(matching_code_sets_unique, recursive=FALSE), function(x) x$SC_CD)
final_counts <- sort(table(unlist(final_codes)), decreasing=TRUE)
print(paste(nrow(predictors), "total codes"))
print("Sample counts for SC codes:")
print(final_counts)

# Get labels for codes that meet the required threshold for instances
used_labels <- keys(counts$getCounts())
used_labels <- used_labels[used_labels!="0"]


# Calculate responses
i<-1
responses <- data.frame()
for(xs in matching_code_sets_unique) {
  cs <- lapply(xs, function(c) c$SC_CD)
  part <- used_labels %in% cs
  for(j in 1:length(used_labels)) {
    responses[i, used_labels[[j]]] <- part[[j]]
  }
  i <- i+1
}


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
  temp[is.na(temp)]<-0
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
  # NA -> 0
  res <- replace_na(res)
  return(res)
}

predictors_eligible <- take_eligible(predictors, string_factors=FALSE)


################################################################################
# Model
################################################################################

require(mlr)

data <- bind_cols(predictors_eligible, responses)

# Make R-standard names
label_names <- make.names(used_labels)
predictor_names <- make.names(colnames(data))

colnames(data) <- predictor_names

train_data <- data[train_vector,]
test_data <- data[test_vector,]

train_task <- makeMultilabelTask(data = train_data, target = unlist(label_names))
test_task <- makeMultilabelTask(data = test_data, target = unlist(label_names))

lrn <- makeLearner("classif.ranger", par.vals=list(
  num.threads = ncores,
  num.trees = 2000
  #sample.fraction = 0.2
))
lrn <- makeMultilabelBinaryRelevanceWrapper(lrn)
lrn <- setPredictType(lrn, "prob")

mod <- mlr::train(lrn, train_task)
pred <- predict(mod, test_task)

mlr::performance(pred, measure <- list(multilabel.hamloss, multilabel.subset01, multilabel.f1))
perf <- getMultilabelBinaryPerformances(pred, measures <- list(mmce, auc))
sorted_perf <- perf[order(perf[,"auc.test.mean"], decreasing=TRUE),]

print(summary(pred$data))

print(counts$getFrequencies())

print(sorted_perf)

extra_stats <- getExtraMultiLabelStats(pred, used_labels, counts)
print(extra_stats)

################################################################################
# Feature Selection
################################################################################

#selected <- feature_selection(train_samples, f_response_train_samples)
#write_features(selected)
