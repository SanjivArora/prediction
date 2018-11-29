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