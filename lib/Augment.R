debugSource("lib/SC.R")

# Add predictors for historical codes
addHistPredictors <- function(cur_predictors, serial_to_pred) {
  # Get the last instance of each code (works due to uniqueBy returning the last matching value)
  getPreviousForRow <- function(row) {
    cs <- getMatchingCodesBefore(serial_to_pred[[row$Serial]], row[,date_field])
    sorted <- sortBy(cs, function(c) c$OCCUR_DATE)
    res <- uniqueBy(cs, function(c) codeToLabel(c))
    return(res)
  }
  
  # Pass in only required values for efficiency
  getPreviousForIndex <- function(i) {
    getPreviousForRow(cur_predictors[i,c("Serial", date_field)])
  }
  
  codes <- values(serial_to_pred) %>% unlist(recursive = FALSE) 
  used_labels <- codes %>% lapply(codeToLabel) %>% unique
  
  previous_code_sets_unique <- plapply(
    1:nrow(cur_predictors),
    getPreviousForIndex,
    parallel=parallel
  )
  
  index_to_hist<- function(i, default_delta=10000) {
    code_set <- previous_code_sets_unique[[i]]
    cs <- groupBy(code_set, function(c) codeToLabel(c))
    part <- list()
    predictor_date <- cur_predictors[i, date_field]
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
  
  hist_predictors_parts <- plapply(1:nrow(cur_predictors), index_to_hist, parallel=parallel)
  hist_predictors <- bindRowsForgiving(hist_predictors_parts)
  colnames(hist_predictors) <- paste("days.since.last", used_labels, sep=".")
  
  predictors <- cbind(cur_predictors, hist_predictors)
  return(predictors)
}

