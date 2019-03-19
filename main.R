# For bartMachine models
#options(java.parameters = "-Xmx128000m")

source("common.R")

require(dplyr)
require(memoise)
require(mlr)
require(itertools)
require(forcats)
require(magrittr)
require(plotly)


data_days <- 100

#sample_rate <- 1 #0.2
sample_rate <- 1
#max_models <- 3

#historical_jam_predictors = FALSE

deltas <- FALSE
delta_days <- c(1, 7)

selected_features <- FALSE


# For generating portable data sets, breaks modeling
#replace_nas=FALSE

device_models <- device_groups[["trial_prod"]]
#device_models <- device_groups[["trial_commercial"]]
#device_models <- device_groups[["e_series_commercial"]]

#device_models <- c("E18")

################################################################################
# Feature Names
################################################################################

if(selected_features) {
  fs <- readFeatures(feature_files)
} else {
  fs <- FALSE
}

################################################################################
# SC & Jam Codes
################################################################################

service_codes <- readCodes(regions, device_models, target_codes, days=data_days, end_date=end_date, parallel=parallel)
serial_to_service_codes <- makeSerialToCodes(service_codes)

jams <- readJamCodes(regions, device_models, target_codes, days=data_days, end_date=end_date, parallel=parallel)
serial_to_jams <- makeSerialToCodes(jams)

serials <- append(service_codes$Serial, jams$Serial) %>% unique
serial_to_codes <- hash()
for (serial in serials){
  a <- getWithDefault(serial_to_service_codes, serial, list())
  b <- getWithDefault(serial_to_jams, serial, list())
  cs <- append(a, b)
  serial_to_codes[[serial]] <- cs
}

################################################################################
# Sample dataset
################################################################################

file_sets <- getEligibleFileSets(regions, device_models, sources, sc_code_days, days=data_days, end_date=end_date)

data_files <- unlist(file_sets)

predictors_all <- dataFilesToDataset(
  data_files,
  sources,
  sample_rate,
  sc_code_days,
  delta_days=delta_days,
  deltas=deltas,
  only_deltas=only_deltas,
  features=fs,
  parallel=parallel
)

###############################################################################
# Clean and condition dataset
################################################################################

predictors <- cleanPredictors(predictors_all, randomize_order=randomize_predictor_order) %>% filterSingleValued

# Stats for dataset
print(paste(nrow(predictors), "total observations"))

###############################################################################
# Build list of matching code sets for each row
################################################################################

matching_code_sets_unique <- getMatchingCodeSets(predictors, serial_to_codes)

################################################################################
# Add predictors for historical codes
################################################################################

if(historical_sc_predictors) {
  predictors <- addHistPredictors(predictors, serial_to_service_codes)
}

if(historical_jam_predictors) {
  predictors <- addHistPredictors(predictors, serial_to_jams)
}

################################################################################
# Restrict to valid numeric values
################################################################################

predictors_eligible <- filterIneligibleFields(predictors, string_factors=factor_fields, exclude_cols=exclude_fields, replace_na=replace_nas)

################################################################################
# Train and test datasets
################################################################################

split_type <- "timeSplit"
splits <- splitPredictors(predictors, split_type, frac=training_frac, sc_code_days=sc_code_days, date_field=date_field)
train_vector <- splits[['train']]
test_vector <- splits[['test']]

###############################################################################
# Choose labels for which we have the best combinations of observations and unique serials
################################################################################

matching_code_sets_unique_train <- matching_code_sets_unique[train_vector]
matching_code_sets_unique_service <- lapply(matching_code_sets_unique_train, function(l) filterBy(l, isServiceCode))
matching_code_sets_unique_jams <- lapply(matching_code_sets_unique_train, function(l) filterBy(l, isJamCode))

used_labels_service <- selectLabels(predictors[train_vector,], matching_code_sets_unique_service, n=max_models)
#used_labels_jams <- selectLabels(predictors[train_vector,], matching_code_sets_unique_jams, n=max_models)
used_labels_jams <- list()

used_labels <- append(used_labels_service, used_labels_jams)

################################################################################
# Generate responses
################################################################################

responses <- generateResponses(matching_code_sets_unique, used_labels)

print("Observation counts for SC codes:")
print(responsesToCounts(responses))

################################################################################
# Model
################################################################################

train_data <- predictors_eligible[train_vector,]
train_responses <- responses[train_vector,]
test_data <- predictors_eligible[test_vector,]
test_responses <- responses[test_vector,]

# Train models in parallel as despite native threading support there are substantial serial sections
models <- trainModelSet(used_labels, train_data, train_responses, ntree=ntree, parallel=F)

################################################################################
# Evaluate performance
################################################################################

stats <- evaluateModelSet(models, test_data, test_responses, parallel=F)
# candidate_stats <- getCandidateModelStats(stats)
# evaluateModelSet(models[candidate_stats$label], test_data, test_responses)

for(label in keys(models)) {
  cat("\n\n")
  print(paste("Importance for", label))
  showModelFeatureImportance(models[[label]], n=10)
}

#print(candidate_stats)

#evaluateModelSet(models[candidate_stats$label], test_data, test_responses, parallel=parallel)

################################################################################
# Save top features
################################################################################

top_features <- topModelsFeatures(models, frac=0.5)
#writeFeatures(row.names(top_features), "top.txt")

################################################################################
# Misc
################################################################################

#plotLatency(predictors_all)

# Plot distribution of predictor values for different model groups
# Get predictors with NAs
#ps_plot <- filterIneligibleFields(predictors, string_factors=factor_fields, exclude_cols=exclude_fields, replace_na=FALSE)
#n<-1
#n<-n+1; plotPredictorsForModels(ps_plot, fs[(1+n*9):(1+(n+1)*9)], c("E15", "E16"), c("E17", "E18"))
#n<-n+1; plotPredictorsForModels(ps_plot, fs[(1+n*9):(1+(n+1)*9)], c("E15"), c("E16"))
#n<-n+1; plotPredictorsForModels(ps_plot, row.names(top_features)[(1+n*9):(1+(n+1)*9)], c("E15", "E16"), c("G69", "G70"))


# Write processed data to s3
# s3saveRDS(predictors, 'data/predictors.RDS', 'ricoh-prediction-misc')
# s3saveRDS(service_codes, 'data/service_codes.RDS', 'ricoh-prediction-misc')
# s3saveRDS(responses, 'data/responses.RDS', 'ricoh-prediction-misc')