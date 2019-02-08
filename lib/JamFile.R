require(anytime)

source('lib/DataFile.R')
  
  JamFile <- setRefClass(
    "JamFile",
    contains = "DataFile",
    methods = list(
      # Get a data frame of all non-N/A Jam codes
      getDataFrame = function() {
        raw <- callSuper(prepend_source=FALSE)
        parts <- list()
        for (i in 1:10) {
          try({
            # Select PaperHistory columns ending with _<i>
            indices <- grepl(paste("PaperHistory.*_", i, "$", sep=""), colnames(raw))
            x <- raw[,indices]
            # Filter out rows with null serials
            x <- x[!is.na(x[,1]),]
            # Strip _<i> suffixes from column names 
            colnames(x) <- lapply(colnames(x), function(n) str_extract(n, '.*(?=_\\d+$)'))
            # Strip PaperHistory_ prefixes from column names 
            colnames(x) <- lapply(colnames(x), function(n) str_extract(n, '(?<=^PaperHistory_).*'))
            # Create Serial column, exploiting fact that row names are set this way by parent class
            x <- cbind(row.names(x), x)
            colnames(x)[1] <- 'Serial'
            # Filter out rows with null codes or dates
            #x <- x[!is.na(x$JAM_CD) & !is.na(x$OCCUR_DATE) & x$JAM_CD != logical(0) & x$OCCUR_DATE != logical(0),]
            #print(x$OCCUR_DATE)
            #Set data types
            x <- transform(x,
              Serial = as.character(Serial),
              PAPER_SIZE_CD = as.character(PAPER_SIZE_CD),
              PAPER_JAM_CD = as.character(PAPER_JAM_CD),
              COUNT_CNTR = as.numeric(COUNT_CNTR),
              #OCCUR_DATE = anytime::anydate(OCCUR_DATE),
              #    #TODO: merge date and time to datetime value
              OCCUR_TIME = as.character(OCCUR_TIME)#,
              #OCCUR_DETAIL = as.character(OCCUR_DETAIL)
            )
            x$OCCUR_DATE = anytime::anydate(x$OCCUR_DATE)
            parts <- append(parts, list(x))
          })
        }
        res <- bindRowsForgiving(parts)
        return(res)
      }
      # TODO: method to extract lifetime counts
    )
  )


# d <- JamFile(path="RNZ_E16_JAM_20180714.csv")
# x <- d$getDataFrame()
  
jamsFor <- function(f, earliest_file_date=NA, latest_file_date=NA, parallel=TRUE) {
  datafiles <- instancesForDir(cls=JamFile)
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

jamsForRegionsAndModels <- function(regions, models, earliest_file_date=NA, latest_file_date=NA, parallel=TRUE) {
  res <- jamsFor(function(f) {f$model %in% models && f$region %in% regions && f$source=='Jam'}, earliest_file_date=earliest_file_date, latest_file_date=latest_file_date, parallel=parallel)
  return(res)
}

# Target_codes is a list of numeric Jam codes identifiers, main code only. Default to all codes.
readJamCodes <- function(regions, models, target_codes=list(1:999), earliest_file_date=NA, latest_file_date=NA, parallel=TRUE) {
  target_codes <- Reduce(append, target_codes)
  target_code_hash <- hash()
  for(c in target_codes) {
    target_code_hash[[as.character(c)]] <- TRUE
  }
  codes_all <- jamsForRegionsAndModels(regions, models, earliest_file_date=earliest_file_date, latest_file_date=latest_file_date, parallel=parallel)
  # Remote duplicate code instances
  code_dups <- duplicated(codes_all[,c('Serial', 'PAPER_SIZE_CD', 'PAPER_JAM_CD', 'OCCUR_DATE')])
  codes_unique <- codes_all[!code_dups,]
  # Filter SC codes to target codes
  code_indices <- plapply(splitDataFrame(codes_unique), function(c) has.key(c$PAPER_JAM_CD, target_code_hash))
  codes <- codes_unique[unlist(code_indices),]
}
