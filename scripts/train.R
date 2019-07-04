source("common.R")

require(dplyr)
require(memoise)
require(mlr)
require(itertools)
require(forcats)
require(magrittr)

# Date of earliest predictor data files to use
#earliest_file_date <- as.Date("2018-11-01")
# Date of last data files to use (including SC data)
#latest_file_date <- as.Date("2018-11-30")

# Number of trees to use for random forest
ntree = 1000
#ntree = 500

################################################################################
# Parse command line argument (sets variables in current environment)
################################################################################

parser <- makeParser()

################################################################################
# Get devices to use, if this is not specified as an argument use default value
################################################################################

device_models <- getDeviceModels()

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

codes <- readCodes(regions, device_models, target_codes, days=data_days, parallel=parallel)
serial_to_codes <- makeSerialToCodes(codes)

jams <- readJamCodes(regions, device_models, target_codes, days=data_days, end_date=end_date, parallel=parallel)
serial_to_jams <- makeSerialToCodes(jams)

################################################################################
# Sample dataset
################################################################################

file_sets <- getEligibleFileSets(regions, device_models, sources, days=data_days, end_date=end_date)
if(!is.na(data_days)) {
  file_sets <- tail(file_sets, data_days)
}

data_files <- unlist(file_sets)
latest_file_date <- latestFileDate(data_files)

predictors_all <- dataFilesToDataset(
  data_files,
  sources,
  sample_rate,
  label_days,
  delta_days=delta_days,
  deltas=deltas,
  only_deltas=only_deltas,
  features=fs,
  parallel=parallel
)
predictors <- predictors_all

###############################################################################
# Clean and condition dataset
################################################################################

predictors <- cleanPredictors(predictors) %>% filterSingleValued

# Stats for dataset
print(paste(nrow(predictors), "total observations"))

###############################################################################
# Build list of matching code sets for each row
################################################################################

matching_code_sets_unique <- getMatchingCodeSets(predictors, serial_to_codes)

###############################################################################
# Choose labels for which we have the best combinations of observations and unique serials 
################################################################################

used_labels <- selectLabels(predictors, matching_code_sets_unique, n=max_models)

################################################################################
# Add predictors for historical codes
################################################################################

if(historical_sc_predictors) {
  predictors <- addHistPredictors(predictors, serial_to_codes)
}

if(historical_jam_predictors) {
  predictors <- addHistPredictors(predictors, serial_to_jams)
}

################################################################################
# Restrict to valid numeric values
################################################################################

predictors_eligible <- filterIneligibleFields(predictors, string_factors=factor_fields, exclude_cols=exclude_fields)

################################################################################
# Generate responses
################################################################################

responses <- generateResponses(matching_code_sets_unique, used_labels)

print("Observation counts for codes:")
print(responsesToCounts(responses))

################################################################################
# Model
################################################################################

train_data <- predictors_eligible
train_responses <- responses

# Train models in parallel as despite native threading support there are substantial serial sections.
# Limit number of threads to keep memory usage in check, however each model will be trained with a full share of cores.
models <- trainModelSet(used_labels, train_data, train_responses, ntree=ntree, parallel=parallel, ncores=(max(1, detectCores() / 8)))

# Save trained model(s)
model_path <- paste(device_group, "/", latest_file_date, ".RDS", sep="")
print(paste("Saving trained models to", paste("s3://", models_s3_bucket, "/", model_path, sep="")))
s3saveRDS(models, model_path, models_s3_bucket)
