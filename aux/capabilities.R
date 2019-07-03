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
require(PTXQC)
require(tibble)


# For toner analysis:
data_days <- 365
label_days <- 0
sc_data_buffer <- 0
randomize_predictor_order <- FALSE

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
      label_days,
      delta_days=delta_days,
      deltas=deltas,
      only_deltas=only_deltas,
      features=FALSE,
      parallel=parallel
    )
  },
  parallel=TRUE
)

dfs <- plapply(dfs, function(x) x %>% distinct(Serial, .keep_all=TRUE))

totals <- hash()
model_capabilities <- hash()
capability_sets <- hash()
capability_set_counts <- hash()
for (i in 1:length(dfs)) {
  #print("")
  name <- model_names[[i]]
  #print(name)
  df <- dfs[[i]]
  #print(nrow(df))
  #print(ncol(df))
  grep('.*Accumulation.*Coverage.*', df %>% names()) -> cov#; names(df)[cov] %>% print
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
      #print(s2)
      #print(names(df[ns]))
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
  if(!length(capability_set)) capability_set <- c('<None / No Data>')
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
