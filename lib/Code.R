
# Quick & Dirty type detection for codes represented as Data Frame rows
isServiceCode <- function(c){
  "SC_CD" %in% names(c) & "SC_SERIAL_CD" %in% names(c)
}

isJamCode <- function(c){
  "PAPER_JAM_CD" %in% names(c) & "PAPER_SIZE_CD" %in% names(c)
}

isReplacement <- function(c) {
  "PART_NAME" %in% names(c)
}

codeToLabel <- function(c) {
  if(isServiceCode(c)){
    label <- paste(c$SC_CD, c$SC_SERIAL_CD, sep='_')
    label <- paste("X", label, sep='')
    # Make this an R-compliant name
    label <- make.names(label)
  } else if(isJamCode(c)){
    label <- paste("J", c$PAPER_JAM_CD, sep='')
    # Make this an R-compliant name
    label <- make.names(label)
  } else if(isReplacement(c)){
    label <- make.names(c$PART_NAME)
  } else{
    stop("Cannot recognise code type")
  }
  return(label)
}

getMatchingCodesBy <- function(codes, f) {
  if(class(codes) == "data.frame") {
    cs <- splitDataFrame(codes)
  } else {
    cs <- codes
  }
  res <- filterBy(cs, f)
  return(res)
}

getMatchingCodes <- function(codes, date, sc_days) {
  f <- function(c) {
    delta <- c$OCCUR_DATE - date
    in_window <- delta > 0 && delta <= sc_days
    return(in_window)
  }
  res <- getMatchingCodesBy(codes, f)
  return(res)
}

getMatchingCodesBefore <- function(codes, date) {
  f <- function(c) {
    delta <- c$OCCUR_DATE - date
    match <- delta < 0
    return(match)
  }
  res <- getMatchingCodesBy(codes, f)
  return(res)
}