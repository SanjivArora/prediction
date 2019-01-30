require(dplyr)
require(memoise)
require(mlr)
require(itertools)
require(forcats)
require(magrittr)
require(testit)
require(aws.ses)
require(xtable)

source("common.R")

# Threshold for inclusion in prediction shortlist file
threshold <- 0.8

# Date of earliest predictor data files to use
#earliest_file_date <- as.Date("2018-11-18")
# Date of last data files to use (including SC data)
#latest_file_date <- as.Date("2018-11-30")

parallel=TRUE

delivery_address <- 'pvanrensburg@ricoh.co.nz'
#delivery_address <- 'smatthews@ricoh.co.nz'

cc_address <- 'smatthews@ricoh.co.nz'

from_address <- 'ricoh-prediction-mail@sdmatthews.com'

aws_ses_region <- 'us-east-1'


################################################################################
# Parse command line argument (sets variables in current environment)
################################################################################

parser <- makeParser()

################################################################################
# Establish an S3 connection so library works correctly with child processes
# (using S3 in parallel fails without this)
################################################################################

bucketlist() %>% invisible

################################################################################
# Get devices to use, if this is not specified as an argument use default value
################################################################################

device_models <- getDeviceModels()

################################################################################
# Load trained model(s)
################################################################################

models_prefix <- paste(device_group, '/', sep='')
models_path <- latestCloudFile(models_s3_bucket, prefix=models_prefix)
models <- s3readRDS(models_path, models_s3_bucket)

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
# SC & Jam Codes
################################################################################

codes <- readCodes(regions, device_models, target_codes, latest_file_date=latest_file_date, parallel=parallel)
serial_to_codes <- makeSerialToCodes(codes)

jams <- readJamCodes(regions, device_models, target_codes, latest_file_date=latest_file_date, parallel=parallel)
serial_to_jams <- makeSerialToCodes(jams)

################################################################################
# Sample dataset
################################################################################

# Get files, set sc_code_days to 0 since we are predicting service codes and therefore don't want to wait for SC data to be available
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
# Write predictions to cloud storage as CSV
################################################################################

predictions_hits <- predictions_narrow[predictions_narrow$Confidence > threshold,]

hits_path <- paste(device_group, paste(latest_file_date, "csv", sep="."), sep='/')
all_path <- paste(device_group, paste(latest_file_date, "all.csv", sep="."), sep='/')
s3write_using(predictions_hits, write.csv, bucket=results_s3_bucket, object=hits_path, opts=list('row.names'=FALSE))
s3write_using(predictions_narrow, write.csv, bucket=results_s3_bucket, object=all_path, opts=list('row.names'=FALSE))


################################################################################
# Email prediction hits to the configured address
################################################################################

# xtable seems to want to represent dates as integers, so explicitly format to string
predictions_hits_formatted <- predictions_hits
predictions_hits_formatted[,c("GetDate", "FileDate")] <- format(predictions_hits_formatted[,c("GetDate", "FileDate")])
# Sort by serial
predictions_hits_formatted <- predictions_hits_formatted[order(predictions_hits_formatted$Serial),]
row.names(predictions_hits_formatted) <- c()
html <- print(xtable(predictions_hits_formatted), type='html')
devices_string <- paste("(", paste(device_models, collapse=", ", sep=""), ")", sep="")

if(!no_email) {
  send_email(
    "",
    html,
    subject=paste("Predictions for", device_group, devices_string),
    to=email_to,
    cc=cc_address,
    from=from_address,
    region=aws_ses_region
  )
}