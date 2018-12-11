sources=c('PMCount', 'Count', 'RomVer') 

feature_files <- c('top.txt')
selected_features <- TRUE

# Number of days of predictor data files to use for training
data_days <- 1000

# Fraction of observations to use
sample_rate <- 1

# Build models for up to this many <SC>_<subcode> pairs
max_models <- 18

# Maximum number of days to predict SC code
sc_code_days <- 14

# Offsets to use for generating deltas for numerical data
delta_days <- c(14)
#delta_days = c(1, 3, 7, 14)

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
ntree = 500

relative_replacement_dates <- TRUE

# String fields to use as factors
factor_fields <- c('Model')

# Fields to exclude
exclude_fields <- c('Serial')