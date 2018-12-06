# Find indices for columns with with positive values
dropSingleLevel <- function(truth) {
  res <- apply(truth, 2, function(c) sum(c) > 0)
  return(res)
}

plotROC <- function(prob, truth) {
  to_graph <- dropSingleLevel(truth)
  prob <- prob[,to_graph]
  truth <- truth[,to_graph]
  roc_pred <- ROCR::prediction(prob, truth)
  roc_perf <- ROCR::performance(roc_pred, 'tpr', 'fpr')
  ROCR::performance(roc_pred, 'auc')
  ROCR::plot(roc_perf, colorize=TRUE)
}

plotPrec <- function(prob, truth) {
  to_graph <- dropSingleLevel(truth)
  prob <- prob[,to_graph]
  truth <- truth[,to_graph]
  roc_pred <- ROCR::prediction(prob, truth)
  roc_perf <- ROCR::performance(roc_pred, 'prec', 'rec')
  ROCR::performance(roc_pred, 'auc')
  ROCR::plot(roc_perf, colorize=TRUE)
}

# Generate stats for each confidence level in levels.
getMultiLabelStats <- function(predictions, truth, levels=c(0.50, 0.60, 0.70, 0.80, 0.90)) {
  col_names <- list("label", "sample.size")
  res_part_names <- list("precision", "recall", "hits")
  # Show levels as percentages for clean display
  for(level in levels*100) {
    for(name in res_part_names) {
      n <- paste(name, ".", level, sep="")
      col_names <- append(col_names, list(n))
    }
  }
  res <- data.frame(matrix(ncol=length(col_names), nrow=1))
  colnames(res) <- col_names
  res[,1] <- as.character(res[,1])
  labels <- names(predictions)
  for(label in labels) {
    sample_size <- sum(truth[,label])
    prob <- predictions[,label]
    r <- list(label, sample_size)
    for(level in levels) {
      prediction <- prob >= level
      true_positives <- prediction & truth[,label]
      false_positives <- prediction & !truth[,label]
      precision <- sum(true_positives) / (sum(true_positives) + sum(false_positives))
      recall <- sum(true_positives) / sum(truth[,label])
      hits <- sum(true_positives)
      r <- append(r, c(precision, recall, hits))
    }
    res <- rbind(res, unlist(r))
  }
  res <- res[2:nrow(res),]
  row.names(res) <- res$label
  res[,2:ncol(res)] <- lapply(res[,2:ncol(res)], as.numeric)
  return(res)
}

getCandidateModelStats <- function(stats) {
  candidates <- stats$precision.80 > 0.75 & !is.na(stats$precision.80)
  res <- stats[candidates,]
  return(res)
}

evaluateModelSet <- function(models, predictors, responses, parallel=TRUE, ncores=NA) {
  predictions <- predictModelSet(models, predictors, parallel=parallel, ncores=ncores)
  
  ps <- values(predictions, simplify=FALSE)
  probs <- lapply(ps, function(pred) pred$data$prob.TRUE)
  prob <- do.call(cbind,probs)
  prob <- as.data.frame(prob)
  truth <- responses[,keys(models)]
  
  stats <- getMultiLabelStats(prob, truth)
  print(stats)
  # Plot precision vs recall for models
  p <- as.matrix(sapply(prob, as.numeric))
  r <- as.matrix(sapply(truth, as.numeric))
  plotPrec(p, r)
  
  # Plot ROC for all models
  #plotROC(p, r)
  
  return(stats)
}

plotPredPrec <- function(pred) {
  p <- as.matrix(sapply(pred$data$prob.TRUE, as.numeric))
  r <- as.matrix(sapply(pred$data$truth, as.numeric))
  plotPrec(p, r)
}

plotPredROC <- function(pred) {
  p <- as.matrix(sapply(pred$data$prob.TRUE, as.numeric))
  r <- as.matrix(sapply(pred$data$truth, as.numeric))
  plotROC(p, r)
}

showModelFeatureImportance <- function(model, n=25) {
  x <- getFeatureImportance(model)
  features_desc <- x$res[, order(x$res[1,], decreasing=T)]
  top_n <- t(features_desc[,1:n])
  print(top_n)
}