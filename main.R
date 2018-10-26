# TODO: for printers with relevant hist codes, generate mean for deltas 2-8 days before failure (one week of data). For printers without failures select random 7 day period from the window under examination.


require(dplyr)
require(memoise)
require(doParallel)
require(parallel)



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

#sources=c('Count', 'PMCount', 'Jam')
sources=c('PMCount', 'Count') 
#sources=c('PMCount') 


library(profvis)
#profvis({
 
# Number of days of predictor data files to use for training
data_days = 7 # 31

# Maximum number of days to predict SC code
sc_code_days = 3

#window_days = 2 #7




regions = c(
  'RNZ'
)

models= c(
  'E16',
  'E15'
)


features = c(
  #'test5.txt'
)

daily_features = c(
  #'pcu_daily.txt',
  #'jobs_daily.txt'
  #'shortlist_daily.txt'
)


nocache = FALSE


use_separate_test_dataset = TRUE

#test_date = get_latest_date(models)
test_date = lubridate::as_date('20180730')

if(use_separate_test_dataset) {
  # Allow a gap of test_data_days
  train_date = test_date - 2*test_data_days
} else {
  train_data = test_date
}

if(test_date < train_date + test_data_days) {
  stop("Test date must be at least data_days after train_date")
}

#test_control_size_multiple = 1
test_control_size_multiple = NULL

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

codes <- codesForRegionsAndModels(regions, models)

################################################################################
# Sample dataset
################################################################################

data_files <- instancesForDir()
data_files_small <- data_files[1:50]
sampleDataset(data_files_small, sources, codes, max_days=sc_code_days)

################################################################################
# Train and test datasets
################################################################################

# Set Seed so that same sample can be reproduced in future
set.seed(101) 

# If we aren't using a separate test dataset, split out train and test
# Select 75% of data for training set, 25% for test set

if(!use_separate_test_dataset) {
  train_predictor_date <- predictor_date(train_date, offset)
  predictors_raw_SC <- read_data(train_predictor_date, models, data_days, sources, fs)
  index_to_code_SC <- read_sc(train_date, models, data_days, offset)
  index_to_code_train_SC <- index_to_code_SC
  index_to_code_test_SC <- index_to_code_SC
  predictors_SC <- get_dataset(train_predictor_date, predictors_raw_SC, index_to_code_SC, window_days, delta_days, offset)
  
  sample_SC <- sample.int(n = nrow(predictors_SC), size = floor(.75*nrow(predictors_SC)), replace = F)
  train_SC <- predictors_SC[sample_SC, ]
  test_SC <- predictors_SC[-sample_SC, ]
  # Otherwise get train and test sets from the specified periods
} else {
  train_predictor_date <- predictor_date(train_date, offset)
  predictors_raw_train_SC <- read_data(train_predictor_date, models, data_days, sources, fs)
  index_to_code_train_SC <- read_sc(train_date, models, data_days, offset)
  train_SC <- get_dataset(train_predictor_date, predictors_raw_train_SC, index_to_code_train_SC, window_days, delta_days, offset)
  test_predictor_date <- predictor_date(test_date, offset)
  predictors_raw_test_SC <- read_data(test_predictor_date, models, test_data_days, sources, fs)
  index_to_code_test_SC <- read_sc(test_date, models, data_days, offset)
  test_SC <- get_dataset(test_predictor_date, predictors_raw_test_SC, index_to_code_test_SC, window_days, test_delta_days, offset, control_size_multiple=test_control_size_multiple)
}

response_train_bool_SC <- row.names(train_SC) %in% keys(index_to_code_train_SC)
response_test_bool_SC <- row.names(test_SC) %in% keys(index_to_code_test_SC)

response_train_SC <- as.numeric(response_train_bool_SC)
response_test_SC <- as.numeric(response_test_bool_SC)

#})
################################################################################
# SC Train
################################################################################

require(randomForest)
require(caret)
require(e1071)
# require(gbm)


### Rendom Forest ###
f_response_train_SC <- as.factor(response_train_SC)
f_response_test_SC <- as.factor(response_test_SC)

# Random forest has issues with training on unbalanced classes, so sample equal classes
n_samples = 1000
positive <- train_SC[response_train_bool_SC,]
negative <- train_SC[!response_train_bool_SC,]
positive_samples <- positive[sample(nrow(positive), n_samples, replace=TRUE),]
negative_samples <- negative[sample(nrow(negative), n_samples, replace=TRUE),]
train_samples <- rbind(positive_samples, negative_samples)
# Work around renaming of duplicated rows
train_samples_rownames <- sub("\\..*$", "", row.names(train_samples))
response_train_samples <- as.numeric(train_samples_rownames %in% keys(index_to_code_train_SC))
f_response_train_samples <- as.factor(response_train_samples)

mtry = 100
nodesize = 5
# Default is 500
#ntree=500
ntree=100
nruns=16


wrap_rf <- function(null) {
  randomForest(
    train_samples,
    f_response_train_samples,
    ntree=ntree
    #mtry=mtry,
    #nodesize=nodesize,
  )
}

r_SC_parts <- plapply(1:nruns, wrap_rf)
r_SC <- do.call(randomForest::combine, r_SC_parts)
#print(r_SC)

################################################################################
# SC Predict
################################################################################

### Random Forest ###

p_SC <- predict(r_SC, test_SC)
f_p_SC <- factor(p_SC, levels=min(response_test_SC):max(response_test_SC))

# Evaluate quality of predictions
cm1_SC <- confusionMatrix(f_p_SC, f_response_test_SC)
print(cm1_SC)


################################################################################
# Feature Selection
################################################################################

#selected <- feature_selection(train_samples, f_response_train_samples)
#write_features(selected)
