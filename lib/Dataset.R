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

get_names <- function (last_date, prefix, files=NA, days=default_days) {
  pattern <- paste("^", prefix, sep="")
  files=list.files(base_path,pattern=pattern)
  matching <- list()
  for (f in files) {
    date <- get_date(f, prefix)
    if(!is.na(date) && date <= last_date) {
      matching <- append(matching, f)
    }
  }
  fs <- sort(unlist(matching), decreasing = TRUE)
  if(length(fs)<days) {
    stop("Not enough data files for selected number of days")
  }
  res <- list()
  for (day in 1:days) {
    f <- fs[day]
    correct_date <- last_date - lubridate::days(day-1)
    date <- get_date(f, prefix)
    if(date != correct_date) {
      stop(paste("Missing file for", correct_date, "for", prefix))
    }
  }
  return(fs[1:days])
}

prefixes_for_model = function(model, sources=default_sources) {
  return (
    map(sources, function (source) paste(model, source, sep="_"))
  )
}

files_for_model <- function (date, model, days=default_days, sources=default_sources) {
  return (map(prefixes_for_model(model, sources=sources), function(model) get_names(date, model, days=days)))
}
#files_for_model <- memoise(files_for_model, cache=fs_cache)

matching_files <- function(files_list){
  length(unique(map(files_list,function (x) substrRight(x,12))))==1
}

# Read CSV file
read_file <- function(name) {
  df <- read.csv(file.path(base_path, name), header = TRUE, na.strings=c("","NA"))
  df <- update_types(df)
  return(df)
}

update_types <- function(df) {
  df <- transform(
    df,
    Serial = as.character(Serial)
  )
}

files_upload <- function (list_of_files) {
  if(matching_files(list_of_files)==TRUE){
    return (map(list_of_files, read_file))
  } else {print("Files dates do not match")}
}

num_columns <- function(data){
  nums <- unlist(lapply(data, is.numeric))
  return(data[,nums])
}

replace_na<-function(data){
  temp <- as.data.frame(data)
  temp[is.na(data)==TRUE]<-0
  return(temp)
}

normalize <- function(data) {
  data <- replace_na(data)
  res<-(data - colMeans(data))/ifelse(colSds(as.matrix(data))==0,1,colSds(as.matrix(data)))
  return(res)
}

# Accept a listing of files for each type, return a list of sets of files containing one for each type
filename_tuples<-function(files_list){
  lengths <- map(files_list, length)
  if (!length(unique(lengths))==1) {
    stop("Mismatching number of files")
  }
  # TODO: check dates match for each file
  n <- length(files_list)
  res <- list()
  for (i in 1:lengths[[1]]) {
    res[[i]]<-list()
    for (j in 1:n) {
      res[[i]][[j]]<-files_list[[j]][[i]]
    }
  }
  return(res)
}

# Accept a set of filenames, load data from each and return dataframe merged on 'Serial'. Set row name to serial number.
load_data <- function(file_set) {
  dataframes <- files_upload(file_set)
  res <- dataframes[[1]]
  if(length(dataframes) >= 2) {
    for (frame in dataframes[2:length(dataframes)]) {
      res <- merge(res, frame, by.x = "Serial",by.y = "Serial")
    }
  }
  row.names(res) <- unlist(res[,'Serial'])
  return(res)
}

dataframes_for_model <- function(date, model, days=default_days, sources=default_sources) {
  fs <- files_for_model(date, model, days=days, sources=sources)
  tuples <- filename_tuples(fs)
  #res <- map(tuples, load_data)
  res <- foreach(x=tuples) %dopar% load_data(x)
  return(res)
} 
#dataframes_for_model <- memoise(dataframes_for_model, cache=fs_cache)

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
