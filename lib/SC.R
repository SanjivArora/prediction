
# Target_codes is a list of numeric SC identifiers, main code only. Default to all codes.
readCodes <- function(regions=NA, models=NA, target_codes=list(1:999), days=NA, end_date=NA, parallel=TRUE) {
  target_codes <- Reduce(append, target_codes)
  target_code_hash <- hash()
  for(c in target_codes) {
    target_code_hash[[as.character(c)]] <- TRUE
  }
  codes_all <- codesForRegionsAndModels(regions, models, days=days, end_date=end_date, parallel=parallel)
  # Remote duplicate code instances
  code_dups <- duplicated(codes_all[,c('Serial', 'SC_CD', 'SC_SERIAL_CD', 'OCCUR_DATE')])
  codes_unique <- codes_all[!code_dups,]
  # Filter SC codes to target codes
  code_indices <- plapply(splitDataFrame(codes_unique), function(c) has.key(c$SC_CD, target_code_hash))
  codes <- codes_unique[unlist(code_indices),]
}

# Get a list of unique matching codes for each observation
getMatchingCodeSets <- function(predictors, serial_to_codes, date_field='GetDate', label_days=14, parallel=TRUE) {
  getMatchingCodesForIndex <- function(i) {
    row <- predictors[i, c("Serial", date_field)]
    getMatchingCodes(
      getWithDefault(serial_to_codes, row$Serial, list()),
      row[,date_field],
      label_days
    )
  }
  
  # Pass in only required values for efficiency
  matching_code_sets <- plapply(
    1:nrow(predictors),
    getMatchingCodesForIndex,
    parallel=parallel
  )
  
  # Get the first instance of each SC (works due to uniqueBy returning the last matching value)
  # Indexing by time would be ideal but probably not worth the added complexity.
  matching_code_sets_sorted <- plapply(
    matching_code_sets,
    function(cs) sortBy(cs, function(c) c$OCCUR_DATE, desc=TRUE),
    parallel=parallel
  )
  
  matching_code_sets_unique <- plapply(
    matching_code_sets_sorted,
    function(cs) uniqueBy(cs, function(c) codeToLabel(c)),
    parallel=parallel
  )
  
  return(matching_code_sets_unique)
}