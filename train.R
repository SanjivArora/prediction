require(dplyr)
require(memoise)
require(mlr)
require(itertools)
require(forcats)
require(magrittr)
require(parallel)

source("common.R")

# Date of earliest predictor data files to use
#earliest_file_date <- as.Date("2018-11-01")
# Date of last data files to use (including SC data)
#latest_file_date <- as.Date("2018-11-30")

# Number of trees to use for random forest
ntree = 1000
#ntree = 500

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

codes <- readCodes(regions, device_models, target_codes, latest_file_date=latest_file_date, parallel=parallel)
serial_to_codes <- makeSerialToCodes(codes)

################################################################################
# Sample dataset
################################################################################

file_sets <- getEligibleFileSets(regions, device_models, sources, sc_code_days, latest_file_date=latest_file_date)

data_files <- unlist(file_sets[1:data_days])

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
# Add predictors for historical SC codes
################################################################################

if(historical_sc_predictors) {
  predictors <- addHistSC(predictors, serial_to_codes)
}

################################################################################
# Restrict to valid numeric values
################################################################################

predictors_eligible <- filterIneligible(predictors, string_factors=factor_fields, exclude_cols=exclude_fields)

################################################################################
# Generate responses
################################################################################

responses <- generateResponses(matching_code_sets_unique, used_labels)

print("Observation counts for SC codes:")
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
model_dir <- 'trained'
model_filename <- 'model'
mkdirs(model_dir)
model_path <- file.path(model_dir, model_filename)
print(paste("Saving trained model to", model_path))
saveRDS(models, model_path)
