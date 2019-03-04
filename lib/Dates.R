require(lubridate)

# Convert date and time to datetime
# Accept either strings or objects that represent as strings ala "2019-02-17" and "03:39:59"
makeDateTimes <- function(dates, times) {
  s <- paste(dates, times)
  ymd_hms(s)
}