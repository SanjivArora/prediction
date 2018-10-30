
#library(anytime)


# Extract date from file name
get_date <- function(filename, prefix=NA) {
  if (is.na(prefix)) {
    r <- "^.*_(\\d{8}).csv$"
  } else {
    r <- paste(paste("^", prefix, sep=""), "(\\d{8}).csv$", sep="_")
  }
  d_string <- str_match(filename, r)[2]
  if(is.na(d_string)) {
    return(NA)
  } else {
    date <- lubridate::as_date(d_string)
    return(date)
  }
}

min_date <- function(dates) {
  structure(min(unlist(dates)), class="Date")
}

max_date <- function(dates) {
  structure(max(unlist(dates)), class="Date")
}

predictor_date <- function(response_date, offset) {
  response_date - offset
}

response_start_date <- function(response_date, offset) {
  lubridate::as_date(response_date) - lubridate::days(offset)
}