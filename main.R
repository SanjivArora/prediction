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

#device_models <- device_groups[["trial_prod"]]
device_models <- device_groups[["trial_commercial"]]
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
# Sample dataset
################################################################################

file_sets <- getEligibleFileSets(regions, device_models, sources, label_days, days=data_days, end_date=end_date)

data_files <- unlist(file_sets)

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

###############################################################################
# Clean and condition dataset
################################################################################

predictors <- cleanPredictors(predictors_all, randomize_order=randomize_predictor_order) %>% filterSingleValued

# Stats for dataset
print(paste(nrow(predictors), "total observations"))

################################################################################
# Part replacements
################################################################################

by_ser <- group_by(predictors, Serial)
by_ser %<>% arrange(desc(RetrievedDateTime))

replacement_idxs <- grep('.*Replacement\\.Date.*(?<!relative)$', predictors %>% names(), perl=TRUE)
replacement_fields <- names(predictors)[replacement_idxs]
replacement_matches <- str_match_all(replacement_fields, '.*Unit\\.Replacement\\.Date\\.(.*)\\.SP7.*')
replacement_part_names <- lapply(replacement_matches, function(x) x[[2]])

replacement_codes <- data.frame()

for(i in 1:length(replacement_fields)) {
  new = sym(paste(replacement_part_names[[i]], ".replaced", sep=""))
  field = sym(replacement_fields[[i]])
  by_ser %<>% mutate(!!new := (!!field != lag(!!field)) & !is.na(lag(!!field)) &!is.na(!!field))
  by_ser %<>% mutate(!!new := ifelse(is.na(!!new), FALSE, !!new))
  # Add replacement pseudo-codes
  hits <- by_ser[which(by_ser[,as_string(new)] == TRUE),]
  if(length(hits$Serial) > 0) {
    # OCCUR_DATE is when we pick up the change, not the registered installation date of the part
    replacement_codes %<>% rbind(data.frame(Serial=hits$Serial, PART_NAME=replacement_part_names[[i]], OCCUR_DATE=hits$GetDate))
  }
}

serial_to_replacements <- makeSerialToCodes(replacement_codes)

# Merge back to original predictors to preserve ordering
predictors <- merge(x = predictors, y = by_ser, by = names(predictors))

################################################################################
# Service and Jam Codes
################################################################################

service_codes <- readCodes(regions, device_models, target_codes, days=data_days, end_date=end_date, parallel=parallel)
serial_to_service_codes <- makeSerialToCodes(service_codes)

jams <- readJamCodes(regions, device_models, target_codes, days=data_days, end_date=end_date, parallel=parallel)
serial_to_jams <- makeSerialToCodes(jams)

################################################################################
# Built list of codes, and replacements which we treat as codes
################################################################################

serials <- append(service_codes$Serial, jams$Serial) %>% append(replacement_codes$serial) %>% unique
serial_to_codes <- hash()
for (serial in serials){
  a <- getWithDefault(serial_to_service_codes, serial, list())
  b <- getWithDefault(serial_to_jams, serial, list())
  c <- getWithDefault(serial_to_replacements, serial, list())
  cs <- append(a, b) %>% append(c)
  serial_to_codes[[serial]] <- cs
}

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
splits <- splitPredictors(predictors, split_type, frac=training_frac, label_days=label_days, date_field=date_field)
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
used_labels_replacements <- replacement_part_names

used_labels <- append(used_labels_service, used_labels_jams) %>% append(used_labels_replacements)

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

stats <- evaluateModelSet(models, test_data, test_responses, parallel=T)
# candidate_stats <- getCandidateModelStats(stats)
# evaluateModelSet(models[candidate_stats$label], test_data, test_responses)

for(label in keys(models)) {
  cat("\n\n")
  print(paste("Importance for", label))
  showModelFeatureImportance(models[[label]], n=15)
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

plotLatency(predictors_all)

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

# Overall stats for service codes
#service_codes <- readCodes(NA, NA, target_codes, days=data_days, end_date=end_date, parallel=parallel)

# Service code frequency
# up_to <- as.Date('2019-05-01')
# weeks <- 52
# service_codes[service_codes$SC_CD %in% c(899) & service_codes$OCCUR_DATE > (up_to - weeks*7)  & service_codes$OCCUR_DATE < up_to & service_codes$Serial & service_codes$Serial %in% z & service_codes$OCCUR_DETAIL=='pcl',]$OCCUR_DATE %>% hist(breaks=80)
# service_codes[service_codes$SC_CD %in% c(899) & service_codes$OCCUR_DATE > (up_to - weeks*7)  & service_codes$OCCUR_DATE < up_to & service_codes$Serial %in% z,]$OCCUR_DATE -> dates
service_codes[service_codes$SC_CD %in% c(899) & service_codes$OCCUR_DATE > (up_to - weeks*7)  & service_codes$OCCUR_DATE < up_to & service_codes$Serial %in% z & service_codes$OCCUR_DETAIL=='pcl',]$OCCUR_DATE -> dates
plot_ly(x=dates, type="histogram", nbinsx=weeks)
service_codes[service_codes$SC_CD==899,c('OCCUR_DETAIL', 'SC_CD')] %>% table(exclude=0) %>% View
#
# 899 cause breakdown
# service_codes <- readCodes()
# service_codes[service_codes$SC_CD==899,c('OCCUR_DETAIL', 'SC_CD')] %>% table() %>% as.data.frame -> x
# x$OCCUR_DETAIL %<>% as.character
# others <- x$Freq < 0.03 * sum(x$Freq)
# others_sum <- x[others,]$Freq %>% sum
# x <- x[!others,]
# x[length(x)+1,] <- c('other', 899, others_sum)
# plot_ly(values=x$Freq, labels=x$OCCUR_DETAIL, type='pie')
 
# require(readxl)
# scheduled <- withCloudFile('s3://ricoh-prediction-misc/scheduled.xlsx', read_excel)
# names(scheduled) <- c("Date", "Serial")
# scheduled %<>% dplyr::distinct(Serial, .keep_all=T)
# hist(scheduled$Date, breaks=40)
# plot_ly(type="histogram", x=scheduled$Date, nbinsx=40)
