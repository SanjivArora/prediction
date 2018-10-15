require('Boruta')
require(plotly)


feature_selection <- function(dataset, response, n=300) {
  b<-Boruta(dataset, response)
  
  ih <- b[['ImpHistory']]
  # Remove shadow metadata
  ih_relevant <- ih[,1:(ncol(ih)-3)]
  ih_noinf <- ih_relevant[,which(colMeans(is.finite(ih_relevant))==1)]
  col_order <- order(colMedians(ih_noinf))
  # Select up to n features, ascending order
  col_order <- tail(col_order, n)
  ordered <- ih_noinf[,col_order]
  columns <- map(1:ncol(ordered), function(x) ordered[,x])
  feature_names <- colnames(ordered)
  
  p <- plot_ly(type='box')
  for(i in 1:length(feature_names)) {
    p <- add_trace(p, y=columns[[i]], name=feature_names[[i]])
  }
  p<-hide_legend(p)
  print(p)
  
  return(feature_names)
}