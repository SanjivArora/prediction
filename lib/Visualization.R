# Plot dimenstionally reduced dataset, optionally taking sets of serials to color

require(dplyr)
require(plotly)
require(magrittr)

remove_na<-function(data, normalize = TRUE){
  data[is.na(data)==TRUE]<-0
  if(normalize ==TRUE) {
    data<-(data- colMeans(data))/ifelse(colSds(as.matrix(data))==0,1,colSds(as.matrix(data)))
  }
  return(data)
}


plot_umap_sc <- function(dataset, color_sets=list()) {
  dataset <- remove_na(dataset)
  
  u<-umap(dataset)

  plot(u$layout[,1],u$layout[,2])#,xlim=c(-5,8))
  
  cols = list()
  for (i in 1:length(color_sets)) {
    for(serial in color_sets[[i]]) {
      cols <- append(cols, list(i))
    }
  }
  
  serials <- unlist(color_sets)
  
  index <- row.names(dataset) %in% serials
  filtered <- u$layout[index,]
  
  points(filtered[,1],filtered[,2], col="red")
}


# Plot daily distribution of data latency - disable filtering of stale and misaligned data for best effect
plotLatency <- function(predictors, model=NA) {
  ps <- predictors
  if(!identical(model, NA)) {
    ps <- ps[ps$Model=="E16",]
  }
  ps$Latency <- ps$FileDate - ps$GetDate
  ps <- ps[,c('FileDate', 'Latency')]
  days <- groupBy(splitDataFrame(ps), function (f) f$FileDate)
  days <- values(days)
  day_latencies <- plapply(days, function(day) lapply(day, function(x) as.integer(x$Latency)) %>% unlist)
  
  day_to_freqs <- function(latencies) {
    fs <- table(latencies)
    res <- data.frame(rep(0, 15))
    res <- t(res)
    res[,(names(fs) %>% as.integer) + 1] <- unname(fs)
    return(res)
  }
  
  day_freqs <- plapply(day_latencies, day_to_freqs)
  freqs <- bind_rows(day_freqs) %>% t
  plot_ly(z=freqs, type='heatmap')
}

overlapingHistogram <- function(v1, v2, title=NA, xlab=NA, breaks=NA) {
  # Histogram Colored (blue and red)
  hist(v1, col=rgb(1,0,0,0.5), main=title, xlab=xlab, breaks=breaks)
  hist(v2, col=rgb(0,0,1,0.5), add=T, breaks=breaks)
  box()
}

# Plot a histogram of values a predictor takes for specified models
plotPredictorForModels <- function(predictors, name, m1, m2, breaks=200) {
  v1 <- predictors[predictors$Model %in% m1,name]
  v2 <- predictors[predictors$Model %in% m2,name]
  v1 %<>% log2
  v2 %<>% log2
  # Hack: if no finite values, add a single zero so histogram creation doesn't fail
  if(!any(is.finite(v1))) {
    v1 %<>% append(0) 
  }
  if(!any(is.finite(v2))) {
    v2 %<>% append(0) 
  }
  print(name)
  print("v1")
  print(summary(v1))
  print("v2")
  print(summary(v2))
  overlapingHistogram(
    v1,
    v2,
    title=paste(
      name,
      paste(
        paste(m1, collapse="+"), 
        paste(m2, collapse="+"), 
        sep=" v "
      ),
      "(R v B)"
    ),
    xlab="Log2 Value",
    breaks=breaks
  )
}

plotPredictorsForModels <- function(predictors, names, m1, m2, n=9, breaks=200) {
  names <- intersect(names, names(predictors))
  names <- filterBy(names, function(x) predictors[,x] %>% is.numeric)
  size <- ceiling(sqrt(n))
  par(mfrow=c(size,size))
  for(name in names[1:n]) {
    plotPredictorForModels(predictors, name, m1, m2, breaks=breaks)
  }
}
