require(mlr)
require(parallel)
require(hash)

source("lib/Logging.R")

model_log <- getModuleLogger("Model")

calculateWeights <- function(responses, positive_share=1, control_share=1, parallel=TRUE){
  model_log$debug("Calculating weights")
  
  counts <- responsesToCounts(responses)
  n_positive <- sum(responsesToPositiveCounts(responses))
  n_train_samples <- nrow(responses)
  
  weightForIndex <- function(i) {
    row <- responses[i,1:ncol(responses),drop=FALSE]
    # Default to control weight
    weight <- control_share / counts[["0"]]
    # Take the maximum class weight if a class is positive
    if(any(unlist(row))) {
      for(k in names(counts)) {
        if(k!="0" && row[,make.names(k)]) {
          weight <- max(weight, positive_share / counts[[k]])
        }
      }
    }
    return(weight)
  }
  
  weights <- plapply(1:nrow(responses), weightForIndex, parallel=parallel)
  weights <- unlist(weights)
  return(weights)
}

trainLabel <- function(label, predictors, response, ntree=500, n_threads=NA) {
  model_log$debug(paste("Preparing to train", label))
  
  data <- bind_cols(predictors, response)
  task <- makeClassifTask(data=data, target=label)
  
  # Calculate weights such that the current positive class has balanced representation with the control class
  target <- getTaskTargets(task)
  target <- target %>% as.logical %>% as.data.frame
  colnames(target) <- label

  weights <- calculateWeights(target, parallel=FALSE)
  
  if(is.na(n_threads)) {
    n_threads <- detectCores() - 1
  }
  
  
  lrn <- makeLearner("classif.ranger", par.vals=list(
    num.threads = n_threads,
    num.trees = ntree,
    importance = 'impurity'
    #sample.fraction = 0.2
  ))
  lrn <- setPredictType(lrn, "prob")
  
  model_log$info(paste("Training", label))
  mod <- mlr::train(lrn, task, weights=weights)
  return(mod)
}

predictWith <- function(mod, predictors) {
  pred <- predict(mod, newdata=predictors)
  return(pred)
}

# Responses must be a labeled dataframe
trainModelSet <- function(labels, data, responses, ntree=500, parallel=TRUE, ncores=NA) {
  model_log$info(paste("Training models for", length(labels), "labels"))
  if(identical(ncores, NA)) {
    ncores <- detectCores()
  }
  n_threads <- max(1, ceiling(detectCores() / min(ncores, length(labels))))
  models <- plapply(
    labels, 
    function(l) {
      trainLabel(l, train_data, train_responses[,l,drop=FALSE], ntree=ntree, n_threads=n_threads)
    },
    parallel=parallel,
    ncores=ncores
  )
  res <- hash(labels, models)
  return(res)
}

# models is a hash mapping labels to models
predictModelSet <- function(models, predictors, parallel=TRUE) {
  labels <- keys(models)
  model_log$info(paste("Predicting with models for", length(labels), "labels"))
  predictions <- plapply(
    values(models, simplify=FALSE),
    function(model) predictWith(model, predictors),
    parallel=parallel
  )
  res <- hash(labels, predictions)
  return(res)
}

# Get labels for codes with the best combination of observations and unique serials
selectLabels <- function(predictors, matching_code_sets_unique, n=10) {
  # Build list of matching code sets for each row
  label_to_row_indices <- hash()
  for(i in 1:nrow(predictors)) {
    cs <- matching_code_sets_unique[[i]]
    for (c in cs) {
      label <- codeToLabel(c)
      indices <- getWithDefault(label_to_row_indices, label, list())
      label_to_row_indices[[label]] <- append(indices, i)
    }
  }
  
  label_counts <- mapHash(label_to_row_indices, function(indices) length(indices))
  
  label_to_unique_serials <- mapHash(
    label_to_row_indices,
    function(row_indices) {
      unique(predictors$Serial[unlist(row_indices)])
    }
  )
  label_to_serial_count <- mapHash(label_to_unique_serials, function(serials) length(serials))
  
  label_to_count_and_unqiues <- mapHashWithKeys(
    label_counts,
    function(label, count) c(count, label_to_serial_count[[label]])
  )
  
  # Use geometric mean of observation count and number of unqiue serials as a figure of merit
  label_to_priority <- mapHash(label_to_count_and_unqiues, geomMean)
  top_labels <- tail(sortBy(label_to_priority, function(x) x), n)
  res <- names(top_labels)
  return(res)
}