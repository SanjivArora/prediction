# Plot dimenstionally reduced dataset, optionally taking sets of serials to color

library(dplyr)

remove_na<-function(data,normalize = TRUE){
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