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
            OCCUR_DATE = as.Date(OCCUR_DATE),
            #    #TODO: merge date and time to datetime value
            OCCUR_TIME = as.character(OCCUR_TIME),
            OCCUR_DETAIL = as.character(OCCUR_DETAIL)
          )
          # Filter out rows with null cpdes or dates
          x <- x[!is.na(x$SC_CD) & !is.na(x$OCCUR_DATE),]

          parts <- append(parts, list(x))
        }
        res <- bind_rows(parts)
        return(res)
      }
      # TODO: method to extract lifetime counts
    )
  )


# d <- SCFile(path="RNZ_E16_SC_20180714.csv")
# x <- d$getDataFrame()
  
codesFor <- function(f) {
  datafiles <- instancesForDir(cls=SCFile)
  datafiles <- filterBy(datafiles, f)
  dfs <- plapply(datafiles, function (x) x$getDataFrame())
  res <- distinct(bind_rows(dfs))
  return(res)
}

codesForRegionsAndModels <- function(regions, models) {
  res <- codesFor(function(f) f$model %in% models && f$region %in% regions && f$source=='SC')
  return(res)
}