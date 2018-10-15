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
      rows <- rbind(rows, x)
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
    Time = as.factor(Time),
    Detail = as.factor(Detail)
  )
  return(rows)
}


process_datasets <- function(dataframes){
  y <- lapply(dataframes, pre_process_datasets)
  z <- reduce(y, rbind)
  return(z)
}


jam_fields <- c(
  "Model",
  "Serial",
  "Code",
  "Size",
  "Volume",
  "Date",
  "Time"
)


pre_process_orig_jam_datasets<-function(df) {
  rows <- data.frame(matrix(ncol=7, nrow=0))
  names(rows) <- jam_fields
  n <-ifelse(is.na(df[,497]),ifelse(is.na(df[,492]),ifelse(is.na(df[,487]),
  ifelse(is.na(df[,482]),ifelse(is.na(df[,477]),ifelse(is.na(df[,472]),
  ifelse(is.na(df[,467]),ifelse(is.na(df[,462]),ifelse(is.na(df[,457]),
  ifelse(is.na(df[,452]),1,1),2),3),4),5),6),7),8),9),10)
  
  for (i in 1:n) {
    # First and second columns plus the nth set of SCCode values
    x <- cbind(df[,1], df[,2], df[,grepl(paste("_",i,"$",sep=""), names(df)) & grepl("OrigHistory", names(df))])
    names(x) <- jam_fields
    if(is.na(data.frame(x[,2])) == FALSE) {
      rows <- rbind(rows, x)
    }
  }
  rows <- transform(
    rows,
    #Serial = as.factor(Serial),
    Code = as.numeric(Code),
    Size = as.factor(Size),
    Volume = as.numeric(Volume),
    Date = as.Date(Date),
    #    #TODO: merge date and time to datetime value
    Time = as.factor(Time)
  )
  return(rows)
}

process_orig_jam_datasets <- function(dataframes){
  y <- lapply(dataframes, pre_process_orig_jam_datasets)
  z <- reduce(y, rbind)
  return(z)
}

pre_process_hist_jam_datasets<-function(df) {
  rows <- data.frame(matrix(ncol=7, nrow=0))
  names(rows) <- jam_fields
  n <-ifelse(is.na(df[,434]),ifelse(is.na(df[,429]),ifelse(is.na(df[,424]),
  ifelse(is.na(df[,419]),ifelse(is.na(df[,414]),ifelse(is.na(df[,409]),
  ifelse(is.na(df[,404]),ifelse(is.na(df[,399]),ifelse(is.na(df[,394]),
  ifelse(is.na(df[,389]),1,1),2),3),4),5),6),7),8),9),10)
  
  for (i in 1:n) {
    # First and second columns plus the nth set of SCCode values
    x <- cbind(df[,1], df[,2], df[,grepl(paste("_",i,"$",sep=""), names(df)) & grepl("PaperHistory", names(df))])
    names(x) <- jam_fields
    if(is.na(data.frame(x[,2])) == FALSE) {
      rows <- rbind(rows, x)
    }
  }
  rows <- transform(
    rows,
    #Serial = as.factor(Serial),
    Code = as.numeric(Code),
    Size = as.factor(Size),
    Volume = as.numeric(Volume),
    Date = as.Date(Date),
    #    #TODO: merge date and time to datetime value
    Time = as.factor(Time)
  )
  return(rows)
}

process_hist_jam_datasets <- function(dataframes){
  y <- lapply(dataframes, pre_process_hist_jam_datasets)
  z <- reduce(y, rbind)
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