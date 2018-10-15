config_dir <- "./config"

read_feature <- function(name) {
  path <- file.path(config_dir, name)
  f <- file(path, open="r")
  lines <- readLines(f)
  trimmed <- lapply(lines, trimws)
  result <- make.names(trimmed)
  return(result)
}

read_features <- function(names) {
  l <- lapply(names, read_feature)
  features <- reduce(l, append, .init=list())
  return(features)
}

read_daily_features <- function(names) {
  features <- read_features(names)
  if(!is_empty(features)) {
    features <- paste(features, "_daily", sep="")
  }
  return(features)
}

write_features <- function(features, name="auto_write.txt") {
  path <- file.path(config_dir, name)
  f <- file(path, open='w')
  write(features, f)
  close(f)
}
