# TODO: for printers with relevant hist codes, generate mean for deltas 2-8 days before failure (one week of data). For printers without failures select random 7 day period from the window under examination.


require(dplyr)
require(memoise)

# MemoiseCache must be loaded first
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


#sources=c('Count', 'PMCount', 'Jam')
sources=c('PMCount', 'Count') 


 
# Number of days of predictor data files to use for training
data_days = 14 #31
# We can calculate deltas for 1 day less than data_days
delta_days = data_days-1
# The number of days to average values over
window_days = 3 #7
# End of data window for code is this many days back from code date, i.e. notionally predict this far into the future
offset = window_days + 2

# data_days to use if we have a separate train and test time periods
test_data_days = 14
test_delta_days = test_data_days-1


model='RNZ_E16'
#m='RNZ_C50'
#m='RNZ_C72'


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

#test_date = get_latest_date(model)
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
# Purge memoization cache
################################################################################

if(nocache) {
  # Run the folowing lines manually for quick purge
  forget(read_data)
  forget(files_for_model)
  forget(dataframes_for_model)
  forget(read_sc)
  # To here
}

################################################################################
# Get Dates
################################################################################

#files <- files_for_model(train_date, model, days=data_days, sources=sources)
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
# Train and test datasets
################################################################################

# Set Seed so that same sample can be reproduced in future
set.seed(101) 

# If we aren't using a separate test dataset, split out train and test
# Select 75% of data for training set, 25% for test set

if(!use_separate_test_dataset) {
  train_predictor_date <- predictor_date(train_date, offset)
  predictors_raw_SC <- read_data(train_predictor_date, model, data_days, sources, fs)
  index_to_code_SC <- read_sc(train_date, model, data_days, offset)
  index_to_code_train_SC <- index_to_code_SC
  index_to_code_test_SC <- index_to_code_SC
  predictors_SC <- get_dataset(train_predictor_date, predictors_raw_SC, index_to_code_SC, window_days, delta_days, offset)
  
  sample_SC <- sample.int(n = nrow(predictors_SC), size = floor(.75*nrow(predictors_SC)), replace = F)
  train_SC <- predictors_SC[sample_SC, ]
  test_SC <- predictors_SC[-sample_SC, ]
  # Otherwise get train and test sets from the specified periods
} else {
  train_predictor_date <- predictor_date(train_date, offset)
  predictors_raw_train_SC <- read_data(train_predictor_date, model, data_days, sources, fs)
  index_to_code_train_SC <- read_sc(train_date, model, data_days, offset)
  train_SC <- get_dataset(train_predictor_date, predictors_raw_train_SC, index_to_code_train_SC, window_days, delta_days, offset)

  test_predictor_date <- predictor_date(test_date, offset)
  predictors_raw_test_SC <- read_data(test_predictor_date, model, test_data_days, sources, fs)
  index_to_code_test_SC <- read_sc(test_date, model, data_days, offset)
  test_SC <- get_dataset(test_predictor_date, predictors_raw_test_SC, index_to_code_test_SC, window_days, test_delta_days, offset, control_size_multiple=test_control_size_multiple)
}

response_train_bool_SC <- row.names(train_SC) %in% keys(index_to_code_train_SC)
response_test_bool_SC <- row.names(test_SC) %in% keys(index_to_code_test_SC)

response_train_SC <- as.numeric(response_train_bool_SC)
response_test_SC <- as.numeric(response_test_bool_SC)


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
ntree=1000
r_SC <- randomForest(
  train_samples,
  f_response_train_samples,
  #mtry=mtry,
  #nodesize=nodesize,
  #ntree=ntree,
)
#r_SC <- randomForest(train_SC, f_response_train_SC)
print(r_SC)
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

selected <- feature_selection(train_samples, f_response_train_samples)
write_features(selected)

# logit_SC <- glm(f_response_train_SC~.,data =  train_SC, family = binomial(link="logit"))
# # summary(logit)
# 
# p_logit_SC <- predict(logit_SC, newdata=test_SC, type = "response")
# 
# 
# f_logit_SC <- factor(as.factor(round(p_logit_SC,digits = 0)), levels=min(response_test_SC):max(response_test_SC))
# 
# # Evaluate quality of predictions
# cm3_1_SC <- confusionMatrix(f_logit_SC, f_response_test_SC)
# print(cm3_1_SC)
# 
# 
# probit_SC <- glm(f_response_train_SC~.,data =  train_SC, family = binomial(link="probit"))
# # summary(probit)
# 
# p_probit_SC <- predict(probit_SC, newdata=test_SC, type = "response")
# 
# f_probit_SC <- factor(as.factor(round(p_probit_SC,digits = 0)), levels=min(response_test_SC):max(response_test_SC))
# 
# # Evaluate quality of predictions
# cm3_2_SC <- confusionMatrix(f_probit_SC, f_response_test_SC)
# print(cm3_2_SC)


# ################################################################################
# # Jam Codes
# ################################################################################
# 
# jam_frames = dataframes_for_model(test_date, model, days=code_days, sources="Jam")
# 
# # jam_codes_hist <- process_hist_jam_datasets(jam_frames)
# # 
# # selected_hist_jams <- filter(
# #   jam_codes_hist,
# #   jam_codes_hist$Code == 3 | jam_codes_hist$Code == 8 | jam_codes_hist$Code == 14 |
# #   jam_codes_hist$Code == 16 | jam_codes_hist$Code == 17 | jam_codes_hist$Code == 51 |
# #   jam_codes_hist$Code == 66 | jam_codes_hist$Code == 67
# # )
# # selected_hist_jams <- filter(selected_hist_jams, !is.na(Serial))
# # selected_hist_jams <- filter(selected_hist_jams, !is.na(Date))
# # oldest_target_code <- oldest_delta + lubridate::days(days + offset)
# # selected_hist_jams <- filter(selected_hist_jams, as.Date(selected_hist_jams$Date) >= as.Date(oldest_target_code))
# 
# 
# jam_codes_orig <- process_orig_jam_datasets(jam_frames)
# 
# selected_orig_jams <- filter(
#   jam_codes_orig,
#   jam_codes_orig$Code == 3 | jam_codes_orig$Code == 8 | jam_codes_orig$Code == 14 |
#     jam_codes_orig$Code == 16 | jam_codes_orig$Code == 17 | jam_codes_orig$Code == 51 |
#     jam_codes_orig$Code == 66 | jam_codes_orig$Code == 67
# )
# selected_orig_jams <- filter(selected_orig_jams, !is.na(Serial))
# selected_orig_jams <- filter(selected_orig_jams, !is.na(Date))
# oldest_target_code <- oldest_delta + lubridate::days(window_days + offset)
# selected_orig_jams <- filter(selected_orig_jams, as.Date(selected_orig_jams$Date) >= as.Date(oldest_target_code))
# 
# ################################################################################
# # Final Jam Datasets
# ################################################################################
# 
# # index_to_code_hist_jams <- get_index_to_code(selected_hist_jams, latest_delta, offset)
# # dataset_hist_including_nas <- get_dataset(filtered_all, index_to_code_hist_jams, window_days, delta_days, latest_delta, offset)
# # dataset_hist <- replace_na(dataset_hist_including_nas)
# # response_hist <- as.numeric(row.names(dataset_hist) %in% keys(index_to_code_hist_jams))
# 
# index_to_code_orig_jams <- get_index_to_code(distinct(selected_orig_jams), latest_delta, offset)
# dataset_orig_including_nas <- get_dataset(filtered_all, (index_to_code_orig_jams), window_days, delta_days, latest_delta, offset)
# dataset_orig <- replace_na(dataset_orig_including_nas)
# response_orig <- as.numeric(row.names(dataset_orig) %in% keys(index_to_code_orig_jams))


################################################################################
# Jam_hist Models
################################################################################


### Rendom Forest ###
# f_response_train_hist <- as.factor(response_train_hist)
# f_response_test_hist <- as.factor(response_test_hist)
# 
# r_hist <- randomForest(train_hist, f_response_train_hist)
# # print(r)
# 
# p_hist <- predict(r_hist, test_hist)
# f_p_hist <- factor(p_hist, levels=min(response_test_hist):max(response_test_hist))
# 
# # Evaluate quality of predictions
# cm1_hist <- confusionMatrix(f_p_hist, f_response_test_hist)
# print(cm1_hist)
# 
# ### Naive Nayes ###
# nb_hist <- naive_bayes(train_hist, f_response_train_hist)
# # print(nb)
# 
# p2_hist <- predict(nb_hist, test_hist)
# f_p2_hist <- factor(p2_hist, levels=min(response_test_hist):max(response_test_hist))
# 
# # Evaluate quality of predictions
# cm2_hist <- confusionMatrix(f_p2_hist, f_response_test_hist)
# print(cm2_hist)
# 
# 
# 
# 
# logit_hist <- glm(f_response_train_hist~.,data =  train_hist, family = binomial(link="logit"))
# # summary(logit)
# 
# p_logit_hist <- predict(logit_hist, newdata=test_hist, type = "response")
# 
# 
# f_logit_hist <- factor(as.factor(round(p_logit_hist,digits = 0)), levels=min(response_test_hist):max(response_test_hist))
# 
# # Evaluate quality of predictions
# cm3_1_hist <- confusionMatrix(f_logit_hist, f_response_test_hist)
# print(cm3_1_hist)
# 
# 
# probit_hist <- glm(f_response_train_hist~.,data =  train_hist, family = binomial(link="probit"))
# # summary(probit)
# 
# p_probit_hist <- predict(probit_hist, newdata=test_hist, type = "response")
# 
# f_probit_hist <- factor(as.factor(round(p_probit_hist,digits = 0)), levels=min(response_test_hist):max(response_test_hist))
# 
# # Evaluate quality of predictions
# cm3_2_hist <- confusionMatrix(f_probit_hist, f_response_test_hist)
# print(cm3_2_hist)


################################################################################
# Jam_orig Models
################################################################################

# 
# ### Rendom Forest ###
# f_response_train_orig <- as.factor(response_train_orig)
# f_response_test_orig <- as.factor(response_test_orig)
# 
# r_orig <- randomForest(train_orig, f_response_train_orig)
# # print(r)
# 
# p_orig <- predict(r_orig, test_orig)
# f_p_orig <- factor(p_orig, levels=min(response_test_orig):max(response_test_orig))
# 
# # Evaluate quality of predictions
# cm1_orig <- confusionMatrix(f_p_orig, f_response_test_orig)
# print(cm1_orig)
# 
# ### Naive Nayes ###
# nb_orig <- naive_bayes(train_orig[,orig_Candidate_Vars], f_response_train_orig)
# # print(nb)
# 
# p2_orig <- predict(nb_orig, test_orig)
# f_p2_orig <- factor(p2_orig, levels=min(response_test_orig):max(response_test_orig))
# 
# # Evaluate quality of predictions
# cm2_orig <- confusionMatrix(f_p2_orig, f_response_test_orig)
# print(cm2_orig)
# 



# logit_orig <- glm(f_response_train_orig~.,data =  train_orig, family = binomial(link="logit"))
# # summary(logit)
# 
# p_logit_orig <- predict(logit_orig, newdata=test_orig, type = "response")
# 
# 
# f_logit_orig <- factor(as.factor(round(p_logit_orig,digits = 0)), levels=min(response_test_orig):max(response_test_orig))
# 
# # Evaluate quality of predictions
# cm3_1_orig <- confusionMatrix(f_logit_orig, f_response_test_orig)
# print(cm3_1_orig)
# 
# 
# probit_orig <- glm(f_response_train_orig~.,data =  train_orig, family = binomial(link="probit"))
# # summary(probit)
# 
# p_probit_orig <- predict(probit_orig, newdata=test_orig, type = "response")
# 
# f_probit_orig <- factor(as.factor(round(p_probit_orig,digits = 0)), levels=min(response_test_orig):max(response_test_orig))
# 
# # Evaluate quality of predictions
# cm3_2_orig <- confusionMatrix(f_probit_orig, f_response_test_orig)
# print(cm3_2_orig)

# plot_umap_SC(dataset, color_sets=response)

################################################################################
# Dimensionality Reduction
################################################################################

# plot(x=umap(dataset)$layout[,1],y=umap(dataset)$layout[,2], col=as.factor(response))



