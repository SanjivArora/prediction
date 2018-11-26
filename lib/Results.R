require(ROCR)

# For use with randomForest library
Results <- setRefClass(
  "Results",
  fields = list(
    prediction = "factor",
    labels = "factor"
  ),
  methods = list(
    getConfusionMatrix = function() confusionMatrix(.self$prediction, .self$labels),
    getPositivePredictiveValue = function() .self$getConfusionMatrix()$byClass[["Pos Pred Value"]],
    getSensitivity = function() .self$getConfusionMatrix()$byClass[["Sensitivity"]],
    getSummary = function() {list(
      PositivePredictiveValue = .self$getPositivePredictiveValue(),
      Sensitivity = .self$getSensitivity()
    )},
    printSummary = function() print(.self$getSummary())
  )
)

plotROC <- function(rf, data, labels) {
  p = predict(rf, type="prob", newdata=data)[,2]
  pred = prediction(p, labels)
  perf = performance(pred,"tpr","fpr")
  plot(perf,main="ROC Curve for Random Forest",col=2,lwd=2)
  abline(a=0,b=1,lwd=2,lty=2,col="gray")
}

# For use with MLR multi-label wrapper results. Generate stats for each confidence level in levels.
getExtraMultiLabelStats <- function(pred, raw_labels, counts, levels=c(0.50, 0.60, 0.70, 0.80, 0.90)) {
  # TODO: Is there a cleaner way to do this?
  col_names <- list("label", "base.frequency", "sample.frequency", "sample.rate", "sample.ratio", "sample.size")
  res_part_names <- list("precision", "recall")
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
  data <- pred$data
  for(raw_label in raw_labels) {
    label=make.names(raw_label)
    truth <- data[, paste("truth.", label, sep="")]
    prob <- data[,paste("prob.", label, sep="")]
    sample_freq <- sum(truth) / nrow(data)
    base_freq <- counts$getFrequencies()[[raw_label]]
    sample_rate <- counts$getSamplingFrequencies()[[raw_label]]
    sample_ratio <- sample_freq / base_freq
    sample_size <- sum(truth)
    r <- list(label, base_freq, sample_freq, sample_rate, sample_ratio, sample_size)
    for(level in levels) {
      prediction <- prob >= level
      true_positives <- prediction & truth
      false_positives <- prediction & !truth
      precision <- sum(true_positives) / (sum(true_positives) + sum(false_positives))
      recall <- sum(true_positives) / sum(truth)
      r <- append(r, c(precision, recall))
    }
    res <- rbind(res, unlist(r))
  }
  res <- res[2:nrow(res),]
  row.names(res) <- res[,"label"]
  res[,2:ncol(res)] <- lapply(res[,2:ncol(res)], as.numeric)
  return(res)
}