# TODO: for printers with relevant hist codes, generate mean for deltas 2-8 days before failure (one week of data). For printers without failures select random 7 day period from the window under examination.


require(dplyr)
require(memoise)
require(doParallel)
require(parallel)
require(mlr)
require(itertools)
require(forcats)


#plapply <- lapply

# MemoiseCache must be loaded first
debugSource("lib/Parallel.R")
debugSource("lib/Util.R")
debugSource("lib/Dates.R")
debugSource("lib/MemoiseCache.R")
debugSource("lib/ReadData.R")
debugSource("lib/Dataset.R")
debugSource("lib/Visualization.R")
debugSource("lib/Feature.R")
debugSource("lib/Processing.R")
debugSource("lib/DatasetProcessing.R")
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
data_days = 1000

# Target samples (will pick up extra samples where there are multiple applicable codes)
total_samples <- 10000

# Maximum number of days to predict SC code
sc_code_days = 14

# Minimum number of sample-days to predict an SC code
min_count=100


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


nocache = FALSE



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
# Get Dates
################################################################################

#files <- files_for_models(train_date, models, days=data_days, sources=sources)
#dates <- lapply(unlist(files), get_date)
#oldest_data <- min_date(dates)
#oldest_delta <- oldest_data + lubridate::days(1)
#latest_delta <- max_date(dates)


################################################################################
# Feature Names
################################################################################

fs <- append(
  read_features(features),
  read_daily_features(daily_features)
)

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

data_files <- instancesForDir()

# Restrict data set for debugging
file_sets <- getDailyFileSets(data_files, sources)
data_files <- unlist(file_sets[1:data_days])

require(profvis)
#profvis({

counts <- dataFilesToCounter(data_files, sources, codes, sc_code_days, min_count, parallel=parallel)
print(counts$getCounts())
counts$setTargetSC(positive_samples)
counts$setTargetControl(control_samples)
predictors <- dataFilesToDataset(data_files, sources, codes, counts, sc_code_days, parallel=parallel)
E#View(predictors[1:5,1:5])
#})

#predictors <- predictors %>% mutate_if(sapply(predictors, is.factor), as.character)


################################################################################
# Train and test datasets
################################################################################

# If we aren't using a separate test dataset, split out train and test
# Select 75% of data for training set, 25% for test set

# Set Seed so that same sample can be reproduced in future
set.seed(101) 

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


i<-1
responses <- data.frame()
for(codes in matching_code_sets_unique) {
  cs <- lapply(codes, function(c) c$SC_CD)
  part <- used_labels %in% cs
  for(j in 1:length(used_labels)) {
    responses[i, used_labels[[j]]] <- part[[j]]
  }
  i <- i+1
}


default_frac <- .75 
# For reference, will encourage memorizing cases
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

# Approximate split of samples by time, oldest first
timeSplit <- function(predictors, frac=default_frac) {
    orders <- order(predictors$GetDate)
    newest <- head(orders, floor(length(orders) * (1-frac)))
    res <- rep(TRUE, nrow(predictors))
    res[newest] <- FALSE
    return(res)
}

# TODO: subsets with unique serials and non-overlapping time ranges (necessarily discards some of the data)


#sample_vector <- serialSplit(predictors)
sample_vector <- timeSplit(predictors)
#sample_vector <- randomSplit(predictors)

train_raw <- predictors[sample_vector, ]
test_raw <- predictors[!sample_vector, ]
train_responses <- responses[sample_vector, ]
test_responses <- responses[!sample_vector, ]
# 
# train_response <- as.numeric(train_response_bool)
# test_response <- as.numeric(test_response_bool)
# 
# factorise <- function(x) {
#   factor(x, levels=min(x):max(x))
# }
# f_train_response <- factorise(train_response)
# f_test_response <- factorise(test_response)


################################################################################
# Restrict to valid numeric values
################################################################################

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
# train <- take_eligible(train_raw)
# test <- take_eligible(test_raw)


################################################################################
# Model
################################################################################

require(mlr)


data <- bind_cols(predictors_eligible, responses)
colnames(data) <- make.names(colnames(data))

train_data <- data[sample_vector,]
test_data <- data[!sample_vector,]

train_task <- makeMultilabelTask(data = train_data, target = unlist(make.names(used_labels)))
test_task <- makeMultilabelTask(data = test_data, target = unlist(make.names(used_labels)))

#lrn <- makeLearner("classif.rpart")
#lrn <- makeLearner("classif.randomForest")
lrn <- makeLearner("classif.ranger", par.vals=list(
  num.threads = ncores,
  num.trees = 1000
  #sample.fraction = 0.2
))
lrn <- makeMultilabelBinaryRelevanceWrapper(lrn)
lrn <- setPredictType(lrn, "prob")

mod <- mlr::train(lrn, train_task)
pred <- predict(mod, test_task)

mlr::performance(pred, measure <- list(multilabel.hamloss, multilabel.subset01, multilabel.f1))
perf <- getMultilabelBinaryPerformances(pred, measures <- list(mmce, auc))
sorted_perf <- perf[order(perf[,"auc.test.mean"], decreasing=TRUE),]
print(sorted_perf)


################################################################################
# Feature Selection
################################################################################

#selected <- feature_selection(train_samples, f_response_train_samples)
#write_features(selected)
