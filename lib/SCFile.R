require(anytime)

source('lib/DataFile.R')
  
  SCFile <- setRefClass(
    "SCFile",
    contains = "DataFile",
    methods = list(
      # Get a data frame of all non-N/A SC codes
      getDataFrame = function() {
        raw <- callSuper(prepend_source=FALSE)
        parts <- list()
        for (i in 1:10) {
          try({
            # Filter out rows with null serials
            x <- raw[!is.na(raw[,'Serial']),]
            # Select SCHistory columns ending with _<i>
            indices <- grepl(paste("SCHistory.*_", i, "$", sep=""), colnames(x))
            x <- x[,indices]
            # Strip _<i> suffixes from column names 
            colnames(x) <- lapply(colnames(x), function(n) str_extract(n, '.*(?=_\\d+$)'))
            # Strip SCHistory_ prefixes from column names 
            colnames(x) <- lapply(colnames(x), function(n) str_extract(n, '(?<=^SCHistory_).*'))
            # Create Serial column, exploiting fact that row names are set this way by parent class
            x <- cbind(row.names(x), x)
            colnames(x)[1] <- 'Serial'
            #Set data types
            x <- transform(x,
              Serial = as.character(Serial),
              SC_CD = as.character(SC_CD),
              SC_SERIAL_CD = as.character(SC_SERIAL_CD),
              SC_TOTAL = as.numeric(SC_TOTAL),
              OCCUR_DATE = anytime::anydate(OCCUR_DATE),
              #    #TODO: merge date and time to datetime value
              OCCUR_TIME = as.character(OCCUR_TIME),
              OCCUR_DETAIL = as.character(OCCUR_DETAIL)
            )
            # Filter out rows with null codes or dates
            x <- x[!is.na(x$SC_CD) & !is.na(x$OCCUR_DATE),]
  
            parts <- append(parts, list(x))
          })
        }
        res <- bindRowsForgiving(parts)
        return(res)
      }
      # TODO: method to extract lifetime counts
    )
  )

# d <- SCFile(path="RNZ_E16_SC_20180714.csv")
# x <- d$getDataFrame()
  
codesFor <- function(f, days=NA, end_date=NA, parallel=TRUE) {
  datafiles <- instancesForBucket(cls=SCFile, end_date=end_date, days=days, sources=c('SC'))
  datafiles <- filterBy(datafiles, f)
  if(length(datafiles)==0) {
    stop("No relevant files found")
  }
  # Filter out results for empty data files
  dfs <- plapply(datafiles, function (x) tryCatch(x$getDataFrame(), error=function(e) NULL), parallel=parallel)
  dfs <- filterBy(dfs, function(x) !is.null(x))
  res <- distinct(bindRowsForgiving(dfs))
  return(res)
}

codesForRegionsAndModels <- function(regions=NA, models=NA, days=NA, end_date=NA, source="SC", parallel=TRUE) {
  res <- codesFor(function(f) {
    res <- f$source==source
    if(!identical(NA, regions)) res %<>% f$region %in% regions
    if(!identical(NA, models)) res %<>% f$model %in% models
    return(res)
  }, days=days, end_date=end_date, parallel=parallel)
  return(res)
}