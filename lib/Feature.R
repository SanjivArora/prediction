config_dir <- "./config"

readFeaturesSingle <- function(name) {
  path <- file.path(config_dir, name)
  f <- file(path, open="r")
  lines <- readLines(f)
  close(f)
  trimmed <- lapply(lines, trimws)
  result <- make.names(trimmed)
  return(result)
}

readFeatures <- function(names) {
  l <- lapply(names, readFeaturesSingle)
  features <- concat(l)
  return(features)
}

writeFeatures <- function(features, name="auto_write.txt") {
  path <- file.path(config_dir, name)
  f <- file(path, open='w')
  write(features, f)
  close(f)
}

canonicalFeatureName <- function(name) {
  # Strip leading and trailing white space
  x <- trimws(name)
  # Make an R-compliant name
  x <- make.names(x)
  # Replace all isntances of multiple periods with single periods
  x <- gsub('\\.{2,}', '\\.', perl=TRUE, x)
  x <- gsub('^\\.', '', perl=TRUE, x)
  x <- gsub('\\.$', '', perl=TRUE, x)
  return(x)
}