require(mlr)
require(hash)
require(testit)
require(magrittr)

source('lib/Util.R')

# Extract feature importance dataframes for dictionary of mlr models
# Return a named list where values are single column dataframes where rows are named for features
getMultilabelFeatureImportance <- function(models) {
  res <- list()
  for(l in keys(models)) {
    m <- models[[l]]
    x <- getFeatureImportance(m)$res
    x <- t(x)
    x <- x[order(x, decreasing=TRUE),]
    res[[l]] <- as.data.frame(x)
  }
  return(res)
}

# Given a feature importance dataframe, return the names of top features
topFeatures <- function(fs, frac=0.1) {
  ord <- order(fs, decreasing=TRUE)
  sorted <- fs[ord,]
  n<-ceiling(frac * length(sorted))
  top <- sorted[1:n]
  top <- as.data.frame(top)
  row.names(top) <- row.names(fs)[ord][1:n]
  return(top)
}

# Merge feature importance lists into a single dataframe by calculating the total importance value for each feature
featureImportanceSums <- function(fs) {
  fs <- unname(fs)
  n <- lapply(fs, function(x) unlist(row.names(x)))
  # Verify all feature lists have the same names
  assert(lapply(n, sort) %>% equal)
  ordered <- lapply(fs, function(x) x[n[[1]],])
  all <- as.data.frame(ordered)
  sums <- base::apply(all, 1, sum)
  sums <- as.data.frame(sums)
  ord <- order(sums, decreasing=TRUE)
  res <- sums[ord,]
  res <- as.data.frame(res)
  row.names(res) <- n[[1]][ord]
  return(res)
}

topModelsFeatures <- function(mod, frac=0.1) {
  mod %>% getMultilabelFeatureImportance %>% featureImportanceSums %>% topFeatures(frac)
}