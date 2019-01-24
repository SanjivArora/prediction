require(dplyr)
require(memoise)
require(mlr)
require(itertools)
require(forcats)
require(magrittr)
require(testit)

source("common.R")

# Threshold for inclusion in prediction shortlist file
threshold <- 0.8

# Date of earliest predictor data files to use
#earliest_file_date <- as.Date("2018-11-18")
# Date of last data files to use (including SC data)
#latest_file_date <- as.Date("2018-11-30")

parallel=TRUE


################################################################################
# Establish an S3 connection so library works correctly with child processes
# (using S3 in parallel fails without this)
################################################################################

bucketlist()

################################################################################
# Load trained model(s)
################################################################################

model_dir <- 'trained'
model_filename <- 'model'
model_path <- file.path(model_dir, model_filename)
models <- readRDS(model_path)

used_labels <- names(models)

################################################################################
# Determine date range
################################################################################

all_data_file_sets <- getEligibleFileSets(regions, device_models, sources, sc_code_days=0, sc_data_buffer=0)
all_valid_data_files <- unlist(all_data_file_sets)
latest_valid_data_file <- sortBy(all_valid_data_files, function(f) f$date, desc=TRUE)[[1]]

if(identical(earliest_file_date, NA)) {
  earliest_file_date <- latest_valid_data_file$date
}
if(identical(latest_file_date, NA)) {
  latest_file_date <- latest_valid_data_file$date
}

assert(earliest_file_date<=latest_file_date)

################################################################################
# SC Codes
################################################################################

codes <- readCodes(regions, device_models, target_codes, latest_file_date=latest_file_date, parallel=parallel)
serial_to_codes <- makeSerialToCodes(codes)

################################################################################
# Sample dataset
################################################################################

# Get files, as we are predicting service codes we naturally don't want to wait for SC data to be available
file_sets <- getEligibleFileSets(regions, device_models, sources, earliest_file_date=earliest_file_date, latest_file_date=latest_file_date, sc_code_days=0, sc_data_buffer=0)

data_files <- unlist(file_sets)

predictors_all <- dataFilesToDataset(
  data_files,
  sources,
  codes,
  sample_rate,
  sc_code_days,
  delta_days=delta_days,
  deltas=deltas,
  only_deltas=only_deltas,
  parallel=parallel
)
predictors <- predictors_all

###############################################################################
# Clean and condition dataset
################################################################################

predictors <- cleanPredictors(predictors)

# Stats for dataset
print(paste(nrow(predictors), "total observations"))

###############################################################################
# Build list of matching code sets for each row
################################################################################

matching_code_sets_unique <- getMatchingCodeSets(predictors, serial_to_codes)

################################################################################
# Add predictors for historical SC codes
################################################################################

if(historical_sc_predictors) {
  predictors <- addHistSC(predictors, serial_to_codes)
}

################################################################################
# Restrict to valid numeric values
################################################################################

predictors_eligible <- filterIneligibleFields(predictors, string_factors=factor_fields, exclude_cols=exclude_fields)

################################################################################
# Impute values for predictors the model expects but which are missing in the data for this time period
################################################################################

model_features <- values(models, simplify=FALSE)[[1]]$learner.model$forest$independent.variable.names
missing <- setdiff(model_features, colnames(predictors_eligible))

if(length(missing) > 0) {
  print(paste("Missing", length(missing), "predictors expected by model in processed data forspecified period"))
  print("Imputing values for missing predictors")
  for(name in missing) {
    predictors_eligible[,name] <- NA
  }
  predictors_eligible <- replaceNA(predictors_eligible)
}

predictors_eligible <- predictors_eligible[,model_features]

################################################################################
# Make predictions
################################################################################

predictions <- predictModelSet(
  models,
  predictors_eligible,
  parallel=FALSE,
  ncores=max(1, detectCores() / 4)
)
hits <- mapHash(predictions, function(p) p$data[p$data$prob.TRUE>0.8,])

################################################################################
# Format predictions for export
################################################################################

# Build single narrow format table
parts <- list()
for(label in names(predictions)) {
  ps <- predictions[[label]]
  part <- predictors[row.names(ps$data), c("Serial","GetDate","FileDate")]
  part[,'Label'] <- label
  part[,'Confidence'] <- ps$data$prob.TRUE
  parts <- append(parts, list(part))
}

predictions_narrow <- bind_rows(parts)

################################################################################
# Write predictions as CSV
################################################################################

predictions_hits <- predictions_narrow[predictions_narrow$Confidence > threshold,]

# Save as CSV
predictions_dir <- 'predictions'
mkdirs(predictions_dir)

hits_path <- file.path(predictions_dir, paste(latest_file_date, "csv", sep="."))
all_path <- file.path(predictions_dir, paste(latest_file_date, "all.csv", sep="."))
write.csv(predictions_hits, file=hits_path, row.names=FALSE)
write.csv(predictions_narrow, file=all_path, row.names=FALSE)
