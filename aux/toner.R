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

# For toner analysis:
data_days <- 365
sc_code_days <- 0
randomize_predictor_order <- FALSE

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

################################################################################
# Restrict to valid numeric values
################################################################################

#predictors_eligible <- filterIneligibleFields(predictors, string_factors=factor_fields, exclude_cols=exclude_fields, replace_na=replace_nas)

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

field <- "PMCount.X7931_11_Toner.Bottle.Bk.Serial.No.SP7.931.011"
grep('.*Toner.Bottle*', predictors %>% names()) -> pos
grep('.*Coverage.*', predictors %>% names()) -> pos
grep('(.*Toner.*)|GetDate.*|Serial', predictors %>% names()) -> pos
#predictors[predictors$Serial==serials[[4]],pos] %>% View

grep('.*Pages.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*End.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Remain.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*PGS.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('Accumulation.*Coverage.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Remaining.Toner*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Toner.Bottle*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Toner.*End.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print

name <- "PMCount.X7931_14_Toner.Bottle.BK.End.SP7.931.014"
name2 <- "PMCount.X7931_19_Toner.Bottle.BK.End.Color.Counter.SP7.931.019"
#predictors_all[,c(name,name2)] %>% View
serials <- predictors$Serial
serial <- serials[[1]]
library(plotly)

traces <- c(
  # Current toner cartridge pages
  "Count.X8891_1_Pages.Current.Toner.BK.SP8.891.001"
  # N-1 cartidge pages
  #"Count.X8901_1_Pages.Current.Toner.BK.previous.SP8.901.001"
  # N-2 cartridge pages (?)
  #"Count.X8911_1_Pages.Toner.Previous.set.Previous.BK.SP8.911.001",
  # Total pages for machine
  #"Count.X8381_1_Total.Total.PrtPGS.SP8.381.001"
)

traces2 <- c(
  # Prod
  #"PMCount.X7931_12_Toner.Bottle.K.Remaining.Toner.SP7.931.012",
  #"PMCount.X7932_12_Toner.Bottle.C.Remaining.Toner.SP7.932.012",
  #"PMCount.X7933_12_Toner.Bottle.M.Remaining.Toner.SP7.933.012",
  #"PMCount.X7934_12_Toner.Bottle.Y.Remaining.Toner.SP7.934.012",
  # Commercial
  #"PMCount.X7931_12_Toner.Bottle.K.Remaining.Toner.SP7.931.012",
  "PMCount.X7932_12_Toner.Bottle.M.Remaining.Toner.SP7.932.012",
  "PMCount.X7933_12_Toner.Bottle.C.Remaining.Toner.SP7.933.012",
  "PMCount.X7934_12_Toner.Bottle.Y.Remaining.Toner.SP7.934.012"
)

traces3 <- c(
  #"Count.X8921_1_Pixel.Coverage.Accumulation.Coverage.BK.SP8.921.001",
  "Count.X8921_2_Pixel.Coverage.Accumulation.Coverage.Y.SP8.921.002", 
  "Count.X8921_3_Pixel.Coverage.Accumulation.Coverage.M.SP8.921.003", 
  "Count.X8921_4_Pixel.Coverage.Accumulation.Coverage.C.SP8.921.004" 
)

traces4 <- c(
  #"Count.X8381_1_Total.Total.PrtPGS.SP8.381.001"
  # Production print version of above:
  #"Count.X8381_1_Total.Total.PrtPGS.SP8.381" 
  "Count.X8921_1_Pixel.Coverage.Accumulation.Coverage.BK.SP8.921.001"
)


makePlotForSerial <- function(serial, traces, yaxis='y1', mode="lines+markers", type="scatter", plot=NA) {
  if(identical(plot, NA)) {
    plot <- plot_ly()
  }
  data <- predictors[predictors$Serial==serial,]
  # Prod
  #xdata <- data$Count.X8381_1_Total.Total.PrtPGS.SP8.381
  # Commercial
  xdata <- data$Count.X8381_1_Total.Total.PrtPGS.SP8.381.001
  #xdata <- data$GetDate
  for(t in traces) {
    plot %<>% add_trace(x=xdata, y=data[,t], type=type, name=t, mode=mode, yaxis=yaxis)#, yaxis=yaxis)
  }
  return(plot)
}

tonerForSerial <- function(serial) {
  p <- plot_ly(width=1400, height=1000)
  p <- makePlotForSerial(serial, traces3, plot=p)
  p %<>% layout(
    title=paste("Toner:", serial),
    #legend = list(x = 100, y = 50),
    #yaxis=list(title="Pages"),
    yaxis2=list(title="Toner%", overlaying="y", side="right", range=c(0,100))
    #yaxis2=list(overlaying="y", side="right")
  )
  p <- makePlotForSerial(serial, traces2, yaxis="y2", plot=p)
  print(p)
}

isLastReading <- function(row) {
}

tonerForSerial(serials[[2035]])
#tonerForSerial("E153MB50516")


################################################################################
# Toner reporting capabilities
################################################################################


sources <- c('Count', 'PMCount')
fs <- instancesForBucket(days=7, end_date=as.Date('2019-03-10'), sources=sources, cls=DataFile, verbose=TRUE, parallel=TRUE)
#fs <- sortBy(fs, function(x) x$model)
#file_sets <- getDailyFileSets(fs, sources)
#model_names <- lapply(file_sets, function(x) x[[1]]$model)

file_groups <- groupBy(fs, function(x) x$model)
model_names <- names(file_groups)

dfs <- plapply(
  values(file_groups),
  function(data_files) {
    dataFilesToDataset(
      data_files,
      sources,
      sample_rate,
      sc_code_days,
      delta_days=delta_days,
      deltas=deltas,
      only_deltas=only_deltas,
      features=FALSE,
      parallel=parallel
    )
  },
  parallel=TRUE
)

totals <- hash()
model_capabilities <- hash()
capability_sets <- hash()
capability_set_counts <- hash()
for (i in 1:length(dfs)) {
  print("")
  name <- model_names[[i]]
  print(name)
  df <- dfs[[i]]
  print(nrow(df))
  #print(ncol(df))
  grep('Accumulation.*Coverage.*', df %>% names()) -> cov#; names(df)[cov] %>% print
  grep('.*Remaining.Toner*', df %>% names()) -> rem; #names(df)[rem] %>% print
  grep('.*Pages.Current.Toner.*', df %>% names()) -> cur#; names(df)[cur] %>% print
  grep('.*Pages.Current.Toner.*prev.*', df %>% names()) -> prev#; names(df)[prev] %>% print
  grep('.*Toner.Bottle.Log5.*', df %>% names()) -> log; #names(df[log]) %>% print
  
  # Check that specified names have a rage of values 
  has_values <- function(ns, n=2) {
    df[,ns] %>% unlist %>% unique %>% length -> l
    l >= n
  }
  
  capability_set <- list()
  process <- function(ns, s, s2) {
    if(length(ns) && has_values(ns)) {
      print(s2)
      print(names(df[ns]))
      totals[[s2]] <- getWithDefault(totals, s2, 0) + nrow(df)
      model_capabilities[[s2]] <- getWithDefault(model_capabilities, s2, list()) %>% append(name)
      capability_set <<- append(capability_set, s2)
    }
  }
  process(cov, 'cov', 'Coverage')
  process(rem, 'rem', 'Toner level')
  process(cur, 'cur', 'Pages for current toner')
  process(prev, 'prev', 'Pages for previous toner')
  process(log, 'log', 'Toner bottle log')
  if(!length(capability_set)) capability_set <- c('<None>')
  capability_set %<>% toString
  capability_sets[[capability_set]] <- getWithDefault(capability_sets, capability_set, list()) %>% append(name) 
  capability_set_counts[[capability_set]] <- getWithDefault(capability_set_counts, capability_set, 0) + nrow(df)
  
}
print(model_capabilities)
print(capability_sets)
print(capability_set_counts)
print(totals)
total_n <- lapply(dfs, nrow) %>% unlist %>% sum
print(paste("Total number of machines with data:", total_n))
