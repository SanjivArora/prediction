debugSource("lib/SC.R")

# Add predictors for historical SC codes
addHistSC <- function(predictors, serial_to_codes) {
  # Get the last instance of each SC code (works due to uniqueBy returning the last matching value)
  getPreviousCodesForRow <- function(row) {
    cs <- getMatchingCodesBefore(serial_to_codes[[row$Serial]], row[,date_field])
    sorted <- sortBy(cs, function(c) c$OCCUR_DATE)
    res <- uniqueBy(cs, function(c) codeToLabel(c))
    return(res)
  }
  
  # Pass in only required values for efficiency
  getPreviousCodesForIndex <- function(i) {
    getPreviousCodesForRow(predictors[i,c("Serial", date_field)])
  }
  
  previous_code_sets_unique <- plapply(
    1:nrow(predictors),
    getPreviousCodesForIndex,
    parallel=parallel
  )
  
  index_to_hist_sc <- function(i, default_delta=10000) {
    code_set <- previous_code_sets_unique[[i]]
    cs <- groupBy(code_set, function(c) codeToLabel(c))
    part <- list()
    predictor_date <- predictors[i, date_field]
    for(label in used_labels) {
      if(has.key(label, cs)) {
        c <- cs[[label]][[1]]
        delta <- predictor_date - c$OCCUR_DATE
      } else {
        delta <- default_delta
      }
      part <- append(part, as.numeric(delta))
    }
    res <- matrix(unlist(part), nrow=1)
    res <- as.data.frame(res)
    return(res)
  }
  
  hist_sc_predictors_parts <- plapply(1:nrow(predictors), index_to_hist_sc, parallel=parallel)
  hist_sc_predictors <- bindRowsForgiving(hist_sc_predictors_parts)
  colnames(hist_sc_predictors) <- paste("days.since.last", used_labels, sep=".")
  
  predictors <- cbind(predictors, hist_sc_predictors)
  return(predictors)
}
