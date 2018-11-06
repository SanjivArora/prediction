# TODO: for printers with relevant hist codes, generate mean for deltas 2-8 days before failure (one week of data). For printers without failures select random 7 day period from the window under examination.


require(dplyr)
require(memoise)
require(doParallel)
require(parallel)
require(mlr)
require(itertools)


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
data_days = 1000 # 31

# Target samples (will pick up extra samples where there are multiple applicable codes)
total_samples <- 1000

# Maximum number of days to predict SC code
sc_code_days = 14


regions = c(
  'RNZ'
)

models= c(
  'E16',
  'E15'
)

parallel=TRUE

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
  200:299, #C01 NOT FUNCTION AT ALL
  600:699, #C01 NOT FUNCTION AT ALL
  800:899, #C01 NOT FUNCTION AT ALL
  900:999 #C01 NOT FUNCTION AT ALL  
)
# Exclude 899
target_codes <- target_codes[target_codes != 899]

target_codes <- Reduce(append, target_codes)
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

parallel=TRUE


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

counts <- dataFilesToCounter(data_files, sources, codes, sc_code_days, parallel=parallel)
print(counts$getCounts())
counts$setTargetSC(positive_samples)
counts$setTargetControl(control_samples)
predictors <- dataFilesToDataset(data_files, sources, codes, counts, sc_code_days, parallel=parallel)
#View(predictors[1:5,1:5])
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
print(paste(nrow(predictors), "total samples"))
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
    newest <- top_n(predictors, floor(frac*nrow(predictors)))
    indices <- !(row.names(predictors) %in% row.names(newest))
    return(indices)
}

# TODO: subsets with unique serials and non-overlapping time ranges (necessarily discards some of the data)


#sample_indices <- serialSplit(predictors)
sample_indices <- timeSplit(predictors)
#sample_indices <- randomSplit(predictors)

train_raw <- predictors[sample_indices, ]
test_raw <- predictors[!sample_indices, ]
train_responses <- responses[sample_indices, ]
test_responses <- responses[!sample_indices, ]
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

take_eligible <- function(dataset) {
  res <- num_columns(dataset)
  res <- replace_na(res)
  return(res)
}

predictors_eligible <- take_eligible(predictors)
# train <- take_eligible(train_raw)
# test <- take_eligible(test_raw)


################################################################################
# Model
################################################################################

#require(randomForest)
#require(caret)
#require(e1071)

# 
# mtry = 100
# nodesize = 5
# # Default is 500
# #ntree=500
# ntree=100
# nruns=16
# 
# #z<-bind_cols(as.data.frame(f_train_response), as.data.frame(f_train_response))
# wrap_rf <- function(null) {
#   randomForest(
#     train,
#     f_train_response,
#     ntree=ntree
#     #mtry=mtry,
#     #nodesize=nodesize,
#   )
# }
# 
# rf_parts <- plapply(1:nruns, wrap_rf)
# rf <- do.call(randomForest::combine, rf_parts)
# #print(r_SC)


require(mlr)


data <- bind_cols(predictors_eligible, responses)
colnames(data) <- make.names(colnames(data))

train_data <- data[sample_indices,]
test_data <- data[!sample_indices,]

train_task <- makeMultilabelTask(data = train_data, target = unlist(make.names(used_labels)))
test_task <- makeMultilabelTask(data = test_data, target = unlist(make.names(used_labels)))

#lrn <- makeLearner("classif.rpart")
lrn <- makeLearner("classif.randomForest")
lrn <- makeMultilabelBinaryRelevanceWrapper(lrn)
lrn <- setPredictType(lrn, "prob")

mod <- mlr::train(lrn, train_task)
pred <- mlr::predict(mod, test_task)

performance(pred, measure <- list(multilabel.hamloss, multilabel.subset01, multilabel.f1))
getMultilabelBinaryPerformances(pred, measures <- list(mmce, auc))


################################################################################
# SC Predict
################################################################################

### Random Forest ###

# p_train <- predict(rf, train)
# p_test <- predict(rf, test)
# #f_p <- factorise(p)
# 
# evaluate <- function(p, f_response) {
#   # Evaluate quality of predictions
#   results <- Results(
#     prediction=p,
#     labels=f_response
#   )
#   print(results$getConfusionMatrix())
#   results$printSummary()
# }
# 
# print("Evaluation on training set:")
# evaluate(p_train, f_train_response)
# print("Evaluation on test set:")
# evaluate(p_test, f_test_response)
# 
# plotROC(rf, test, f_test_response)
################################################################################
# Feature Selection
################################################################################

#selected <- feature_selection(train_samples, f_response_train_samples)
#write_features(selected)
