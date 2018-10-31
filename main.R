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
debugSource("lib/Logging.R")
debugSource("lib/DataFile.R")


#sources=c('Count', 'PMCount', 'Jam')
sources=c('PMCount', 'Count') 
#sources=c('PMCount') 


library(profvis)
#profvis({
 
# Number of days of predictor data files to use for training
data_days = 7 # 31

# Maximum number of days to predict SC code
sc_code_days = 1


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


use_separate_test_dataset = TRUE

#test_date = get_latest_date(models)
test_date = lubridate::as_date('20180730')

# if(use_separate_test_dataset) {
#   # Allow a gap of test_data_days
#   train_date = test_date - 2*test_data_days
# } else {
#   train_data = test_date
# }
# 
# if(test_date < train_date + test_data_days) {
#   stop("Test date must be at least data_days after train_date")
# }

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


# Set Seed so that same sample can be reproduced in future
set.seed(101) 

data_files <- instancesForDir()

# Restrict data set for debugging
file_sets <- getDailyFileSets(data_files, sources)
data_files <- unlist(file_sets[1:3])


require(profvis)
#profvis({
counts <- dataFilesToCounter(data_files, sources, codes, sc_code_days, parallel=parallel)
print(counts$getCounts())
counts$setTargetSC(100)
counts$setTargetControl(100)
predictors <- dataFilesToDataset(data_files, sources, codes, counts, sc_code_days, parallel=TRUE)
#View(predictors[1:5,1:5])
#})

predictors <- predictors %>% mutate_if(sapply(predictors, is.factor), as.character)
predictors <- replace_na(predictors)

################################################################################
# Train and test datasets
################################################################################

# If we aren't using a separate test dataset, split out train and test
# Select 75% of data for training set, 25% for test set


sample_indices <- sample.int(n = nrow(predictors), size = floor(.75*nrow(predictors)), replace = F)
train <- predictors[sample_indices, ]
test <- predictors[-sample_indices, ]


response_train_bool <- train$Serial %in% keys(counts$getSerialToCodes())
response_test_bool <- test$Serial %in% keys(counts$getSerialToCodes())

response_train <- as.numeric(response_train_bool)
response_test <- as.numeric(response_test_bool)

factorise <- function(x) {
  factor(x, levels=min(x):max(x))
}
f_response_train <- factorise(response_train)
f_response_test <- factorise(response_test)

################################################################################
# Model
################################################################################

require(randomForest)
require(caret)
require(e1071)


mtry = 100
nodesize = 5
# Default is 500
#ntree=500
ntree=100
nruns=16


wrap_rf <- function(null) {
  randomForest(
    train,
    f_response_train,
    ntree=ntree
    #mtry=mtry,
    #nodesize=nodesize,
  )
}

rf_parts <- plapply(1:nruns, wrap_rf)
rf <- do.call(randomForest::combine, rf_parts)
#print(r_SC)

################################################################################
# SC Predict
################################################################################

### Random Forest ###

p <- predict(rf, test)
f_p <- factor(p, levels=min(response_test):max(response_test))

# Evaluate quality of predictions
cm1 <- confusionMatrix(f_p, f_response_test)
print(cm1)


################################################################################
# Feature Selection
################################################################################

#selected <- feature_selection(train_samples, f_response_train_samples)
#write_features(selected)
