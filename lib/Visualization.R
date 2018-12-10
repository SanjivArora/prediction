# Plot dimenstionally reduced dataset, optionally taking sets of serials to color

require(dplyr)
require(plotly)

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