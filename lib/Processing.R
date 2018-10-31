require(stringr)

fields <- c(
  "Model",
  "Serial",
  "Code",
  "SC_Serial",
  "Volume",
  "Date",
  "Time",
  "Detail"
)


pre_process_datasets<-function(df) {
  rows <- data.frame(matrix(ncol=8, nrow=0))
  names(rows) <- fields
  n <-ifelse(is.na(df[,60]),ifelse(is.na(df[,54]),ifelse(is.na(df[,48]),
             ifelse(is.na(df[,42]),ifelse(is.na(df[,36]),ifelse(is.na(df[,30]),
             ifelse(is.na(df[,24]),ifelse(is.na(df[,18]),ifelse(is.na(df[,12]),
             ifelse(is.na(df[,6]),1,1),2),3),4),5),6),7),8),9),10)
  for (i in 1:n) {
    # First and second columns plus the nth set of SCCode values
    x <- cbind(df[,1], df[,2], df[,grep(paste("_",i,"$",sep=""), names(df))])
    names(x) <- fields
    if(is.na(data.frame(x[,2])) == FALSE) {
      rows <- bind_rows(rows, x)
    }
  }
  rows <- transform(
    rows,
    #Serial = as.factor(Serial),
    Code = as.numeric(Code),
    SC_Serial = as.factor(SC_Serial),
    Volume = as.numeric(Volume),
    Date = as.Date(Date),
    #    #TODO: merge date and time to datetime value
    Time = as.character(Time),
    Detail = as.character(Detail)
  )
  return(rows)
}


process_datasets <- function(dataframes){
  y <- lapply(dataframes, pre_process_datasets)
  z <- bind_rows(y)
  return(z)
}


get_index_to_code <- function(codes, offset) {
  index_to_code<-hash()
  # Filter out rows with NA code field
  filtered <- codes[complete.cases(codes[,"Code"]),]
  for(j in 1:nrow(codes)) {
    code <- codes[j,]
    start_date <- response_start_date(code$Date, offset)
    serial <- as.character(code$Serial)
    # Unique index for this instance of the code
    index <- paste(serial, code$Code, start_date, sep="-")
    index_to_code[[index]] <- code
  }
  return(index_to_code)
}