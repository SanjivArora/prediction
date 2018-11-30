require(anytime)

source('lib/DataFile.R')
  
  SCFile <- setRefClass(
    "SCFile",
    contains = "DataFile",
    methods = list(
      # Get a data frame of all non-N/A SC codes
      getDataFrame = function() {
        raw <- callSuper()
        parts <- list()
        for (i in 1:10) {
          # Select SCHistory columns ending with _<i>
          indices <- grepl(paste("SCHistory.*_", i, "$", sep=""), colnames(raw))
          x <- raw[,indices]
          # Filter out rows with null serials
          x <- x[!is.na(x[,1]),]
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
        }
        res <- bindRowsForgiving(parts)
        return(res)
      }
      # TODO: method to extract lifetime counts
    )
  )


# d <- SCFile(path="RNZ_E16_SC_20180714.csv")
# x <- d$getDataFrame()
  
codesFor <- function(f, earliest_file_date=NA, latest_file_date=NA, parallel=TRUE) {
  datafiles <- instancesForDir(cls=SCFile)
  if(!identical(earliest_file_date, NA)) {
    datafiles <- filterBy(datafiles, function(f) f$date >= earliest_file_date)
  }
  if(!identical(latest_file_date, NA)) {
    datafiles <- filterBy(datafiles, function(f) f$date <= latest_file_date)
  }
  datafiles <- filterBy(datafiles, f)
  dfs <- plapply(datafiles, function (x) x$getDataFrame(), parallel=parallel)
  res <- distinct(bindRowsForgiving(dfs))
  return(res)
}

codesForRegionsAndModels <- function(regions, models, earliest_file_date=NA, latest_file_date=NA, parallel=TRUE) {
  res <- codesFor(function(f) {f$model %in% models && f$region %in% regions && f$source=='SC'}, earliest_file_date=earliest_file_date, latest_file_date=latest_file_date, parallel=parallel)
  return(res)
}