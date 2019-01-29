getDeviceModels <- function(...) {
  name <- getDeviceModelSetName(...)
  res <- device_groups[[name]]
  return(res)
}

getDeviceModelSetName <- function(default="trial_commercial") {
  args = commandArgs(trailingOnly=TRUE)
  if(length(args) > 0) {
    res <- args[[1]]
  } else {
    res <- default
  }
  return(res)
}