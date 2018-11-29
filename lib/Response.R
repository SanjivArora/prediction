codeSetToLabels <- function(code_set) {
  cs <- lapply(code_set, function(c) codeToLabel(c))
  part <- used_labels %in% cs
  res <- matrix(part, nrow=1)
  res <- as.data.frame(res)
  return(res)
}

generateResponses <- function(matching_code_sets_unique, labels, parallel=TRUE) {
  responses_parts <- plapply(matching_code_sets_unique, codeSetToLabels, parallel=parallel)
  responses <-bindRowsForgiving(responses_parts)
  # Make R-standard names
  label_names <- make.names(labels)
  colnames(responses) <- label_names
  return(responses)
}

responsesToPositiveCounts <- function(responses) {sapply(responses, sum)}

responsesToControlCount <- function(responses) {sum(apply(responses, 1, Negate(any)))}

responsesToCounts <- function(responses) {
  counts <- responsesToPositiveCounts(responses)
  counts[["0"]] <- responsesToControlCount(responses)
  return(counts)
}