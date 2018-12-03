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

# Maximum number of days to predict SC code
sc_code_days <- 14
#sc_code_days=2

parallel=TRUE
#parallel=FALSE

date_field <- 'GetDate'

regions = c(
  'RNZ'
)

device_models= c(
  'E15',
  'E16'
  #'E17',
  #'E18'
  #'E19'
  # Exclude G models for now as counter names and SC subcodes differ 
  #'G69',
  #'G70'
  # TODO: check with Karl whether these are equivalent to E17 and E19 per Rotem's data
  #'G71',
  #'G73'
)

################################################################################
# Read predictions
################################################################################

fs_all <- list.files('predictions', full.names=TRUE)
fs <- fs_all[grepl('.*\\d.csv', fs_all)]

parts <- lapply(fs, read.csv)
preds <- rbind_list(parts)
preds <- as.data.frame(preds)
preds$GetDate <- as.Date(preds$GetDate)
preds$FileDate <- as.Date(preds$FileDate)

################################################################################
# Get codes
################################################################################

codes <- readCodes(regions, device_models, target_codes, parallel=parallel)
serial_to_codes <- makeSerialToCodes(codes)

################################################################################
# Find number of days until code occurring
################################################################################

matching <- getMatchingCodeSets(preds, serial_to_codes, date_field=date_field, sc_code_days=.Machine$integer.max, parallel=FALSE)
preds$all <- matching
preds$matching_code_date <- NA
preds$first_code_date <- NA
preds$first_code <- NA

code_dates <- list()
first_code_date <- list()
first_codes <- list()
for(r in splitDataFrame(preds)) {
  cs <- r$all[[1]]
  code_date <- NA
  first_code <- NA
  first_code_date <- NA
  if(length(cs)!=0) {
    cs <- sortBy(cs, function(c) c$OCCUR_DATE)
    first_code <- codeToLabel(cs[[1]])
    first_code_date <- cs[[1]]$OCCUR_DATE
    for(c in cs) {
      if(codeToLabel(c)==r$Label) {
        code_date <- c$OCCUR_DATE
      }
    }
  }
  code_dates <- append(code_dates, code_date)
  first_codes <- append(first_codes, first_code)
  first_code_date <- append(first_code_date, first_code_date)
}
preds$code_date <- unlist(code_dates)
preds$first_codes <- unlist(first_codes)
preds$first_code_date <- unlist(first_code_dates)
