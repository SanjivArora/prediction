require(purrr)
require(matrixStats)
require(chron)
require(lubridate)
require(umap)
require(iotools)
require(memoise)
require(data.table)
require(stringr)
require(doParallel)

#source("Util.R")
#source("Dates.R")
source("lib/DataFile.R")
source("lib/Util.R")

default_sources=c('Count', 'PMCount')
default_days=7
base_path="~/data/"


select_complete <- function(df) {
  df[complete.cases(df),]
}

get_latest_date <- function(prefix="") {
  files <- list.files(base_path, pattern=paste("^", prefix, ".*", sep=""))
  dates <- lapply(files, get_date)
  dates <- dates[!is.na(dates)]
  latest <- max(unlist(dates))
  latest <- lubridate::as_date(latest)
  return(latest)
}


num_columns <- function(data){
  nums <- unlist(lapply(data, is.numeric))
  return(data[,nums])
}

replace_na<-function(data){
  temp <- as.data.frame(data)
  temp[is.na(temp)]<-0
  return(temp)
}

normalize <- function(data) {
  data <- replace_na(data)
  res<-(data - colMeans(data))/ifelse(colSds(as.matrix(data))==0,1,colSds(as.matrix(data)))
  return(res)
}

dataframes_for_model <- function(date, model, days=default_days, sources=default_sources) {
  first_date = date - lubridate::days(days-1)
  datafiles <- instancesForDir()
  datafiles <- filterBy(datafiles, function(x) x$region==region && x$model==m && x$date >= first_date && x$date <=date && x$source %in% sources)
  res <- plapply(datafiles, function (x) x$getDataFrame())
  return(res)
} 
#dataframes_for_model <- memoise(dataframes_for_models, cache=fs_cache)

dataframes_for_models <- function(date, models, days=default_days, sources=default_sources) {
  by_model <- map(models, function(model) dataframes_for_model(date, model, days, sources))
  lengths <- map(by_model, length)
  if(length(unique(lengths))!=1) {
    stop("Number of days of data differs by model")
  }
  l <- lengths[[1]]
  res <- replicate(l, list())
  for(m_data in by_model) {
    for(i in 1:l) {
      res[[i]] <- append(res[[i]], list(m_data[[i]]))
    }
  }
  res <- map(res, bindRowsForgiving)
  # TODO: make unique by serial (take first), warn about duplicate serials
  # Reset row names
  res <- plapply(res, function (dt) {
    row.names(dt) <- unlist(dt[,'Serial'])
    return(dt)
  })
  return(res)
}

common_serials <- function(dataframes) {
  serials <- map(dataframes, function(df) df["Serial"])
  common <- Reduce(intersect, lapply(serials, unlist))
  return(common)
}

# Accept dataframes ordered from most recent to least recent, return first to n-1-th data table augmented with deltas for numerical columns. These have an "_Daily" suffix.
augment_with_deltas <- function(dataframes) {
  serials <- common_serials(dataframes)
  latest <- dataframes[[1]][serials,]
  res <- list()
  for (prev in dataframes[2:length(dataframes)]) {
    prev <- prev[serials,]
    #TODO: check column types match
    a<-num_columns(latest)
    b<-num_columns(prev)
    delta <- (a - b)
    colnames(delta) <- paste(colnames(delta),"_daily",sep="")
    res <- append(res, list(cbind(latest, delta)))
    latest <- prev
  }
  # Convert to data tables
  #res <- lapply(res, as.data.table)
  return(res)
}
