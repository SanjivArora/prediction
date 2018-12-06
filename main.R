require(dplyr)
require(memoise)
require(mlr)
require(itertools)
require(forcats)
require(magrittr)

# MemoiseCache must be loaded first
debugSource("lib/Parallel.R")
debugSource("lib/Util.R")
debugSource("lib/Dates.R")
debugSource("lib/MemoiseCache.R")
debugSource("lib/Visualization.R")
debugSource("lib/Feature.R")
debugSource("lib/FeatureSelection.R")
debugSource("lib/Feature.R")
debugSource("lib/SCFile.R")
debugSource("lib/Sampling.R")
debugSource("lib/Logging.R")
debugSource("lib/DataFile.R")
debugSource("lib/Model.R")
debugSource("lib/Results.R")
debugSource("lib/Evaluate.R")
debugSource("lib/Cleaning.R")
debugSource("lib/SC.R")
debugSource("lib/Augment.R")
debugSource("lib/Response.R")
debugSource("lib/Split.R")


#sources=c('Count', 'PMCount', 'Jam')
sources=c('PMCount', 'Count') 
#sources=c('PMCount') 

feature_files <- c('top.txt')
selected_features <- FALSE

# Number of days of predictor data files to use for training
data_days <- 1000

# Fraction of observations to use
sample_rate <- 1

# Build models for up to this many <SC>_<subcode> pairs
max_models <- 16

# Maximum number of days to predict SC code
sc_code_days <- 14
#sc_code_days=2

# Offsets to use for generating deltas for numerical data
delta_days <- c(14)
#delta_days = c(1, 2)

# Fraction of days to use for training (less SC overlap window)
training_frac = 0.9

historical_sc_predictors <- TRUE
deltas <- FALSE

# Drop non-delta numerical values
only_deltas <- FALSE

regions = c(
  'RNZ'
)

device_models= c(
  'E15',
  'E16'
  #'E17',
  #'E18',
  #'E19'
  # Exclude G models for now as counter names and SC subcodes differ 
  #'G69',
  #'G70'
  # TODO: check with Karl whether these are equivalent to E17 and E19 per Rotem's data
  #'G71',
  #'G73'
)

parallel=TRUE
#parallel=FALSE

# Specify target codes. Note: prediction is based on a <code>_<subcode> label, currently we don't filter on subcode.
# target_codes <- list(
#   200:299, # "C01 NOT FUNCTION AT ALL"
#   600:699, # "C01 NOT FUNCTION AT ALL"
#   800:899, # "C01 NOT FUNCTION AT ALL
#   900:999 # "C01 NOT FUNCTION AT ALL"  
# )
# Exclude 899
target_codes <- 1:999
exclude_codes <- c()
#exclude_codes <- c(101, 681, 701, 792, 816, 899)
target_codes <- target_codes[!(target_codes %in% exclude_codes)]

date_field <- 'GetDate'

# Number of trees to use for random forest
#ntree = 1000
ntree = 500

relative_replacement_dates <- TRUE

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

predictors_eligible <- filterIneligible(predictors, string_factors=c('Model'))

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

stats <- evaluateModelSet(models, test_data, test_responses)
candidate_stats <- getCandidateModelStats(stats)
# evaluateModelSet(models[candidate_stats$label], test_data, test_responses)

for(label in candidate_stats$label) {
  cat("\n\n")
  print(paste("Importance for", label))
  showModelFeatureImportance(models[[label]])
}

print(candidate_stats)

################################################################################
# Save top features
################################################################################

# top_features <- topMultilabelModelFeatures(mod, frac=0.4)
# writeFeatures(row.names(top_features), "top.txt")