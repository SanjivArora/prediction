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

evaluateModelSet <- function(models, predictors, responses) {
  predictions <- mapHashWithKeys(
    models,
    function(label, model) {
      predictWith(model, predictors, responses[,label,drop=FALSE])
    }
  )
  
  ps <- values(predictions, simplify=FALSE)
  probs <- lapply(ps, function(pred) pred$data$prob.TRUE)
  truths <- lapply(ps, function(pred) pred$data$truth)
  
  prob <- cbind(probs)
  truths <- cbind(truths)
              
  # Plot precision vs recall for models
  p <- as.matrix(sapply(prob, as.numeric))
  r <- as.matrix(sapply(truths, as.numeric))
  plotPrec(p, r)
  
  # Plot ROC for all models
  #plotRoc(p, r)
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