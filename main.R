require(dplyr)
require(memoise)
require(mlr)
require(itertools)
require(forcats)
require(magrittr)
require(plotly)

debugSource("defaults.R")


sample_rate <- 1

selected_features <- FALSE
#' device_models= c(
#'   'E15',
#'   'E16',
#'   'E17',
#'   'E18'
#'   #'E19'
#'   # Exclude G models for now as counter names and SC subcodes differ 
#'   #'G69',
#'   #'G70'
#'   # TODO: check with Karl whether these are equivalent to E17 and E19 per Rotem's data
#'   #'G71',
#'   #'G73'
#' )

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

codes <- readCodes(regions, device_models, target_codes, parallel=parallel)
serial_to_codes <- makeSerialToCodes(codes)

################################################################################
# Sample dataset
################################################################################

file_sets <- getEligibleFileSets(regions, device_models, sources, sc_code_days)

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

###############################################################################
# Clean and condition dataset
################################################################################

predictors <- cleanPredictors(predictors_all) %>% filterSingleValued

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
# Train and test datasets
################################################################################

split_type <- "timeSplit"
splits <- splitPredictors(predictors, split_type, frac=training_frac, sc_code_days=sc_code_days, date_field=date_field)
train_vector <- splits[['train']]
test_vector <- splits[['test']]

################################################################################
# Model
################################################################################

train_data <- predictors_eligible[train_vector,]
train_responses <- responses[train_vector,]

# Train models in parallel as despite native threading support there are substantial serial sections
models <- trainModelSet(used_labels, train_data, train_responses, ntree=ntree, parallel=parallel)

test_data <- predictors_eligible[test_vector,]
test_responses <- responses[test_vector,]

################################################################################
# Evaluate performance
################################################################################

stats <- evaluateModelSet(models, test_data, test_responses, parallel=F)
candidate_stats <- getCandidateModelStats(stats)
# evaluateModelSet(models[candidate_stats$label], test_data, test_responses)

for(label in candidate_stats$label) {
  cat("\n\n")
  print(paste("Importance for", label))
  showModelFeatureImportance(models[[label]])
}

print(candidate_stats)

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

#n=0
#plotPredictorsForModels(predictors_eligible, fs[(1+n*9):(1+(n+1)*9)], c("E15", "E16"), c("E17", "E18"))