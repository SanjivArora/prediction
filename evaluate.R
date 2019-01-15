require(dplyr)
require(memoise)
require(mlr)
require(itertools)
require(forcats)
require(magrittr)
require(rstudioapi)

source("common.R")


################################################################################
# Read predictions
################################################################################

fs_all <- list.files('predictions', full.names=TRUE)
fs <- fs_all[grepl('.*\\d.csv', fs_all)]

parts <- lapply(fs, read.csv)
preds <- rbind_list(parts)
preds <- as.data.frame(preds)
preds$GetDate <- as.Date(preds$GetDate)
preds$FileDate <- as.Date(preds$FileDate)

################################################################################
# Get codes
################################################################################

codes <- readCodes(regions, device_models, parallel=parallel)
serial_to_codes <- makeSerialToCodes(codes)

################################################################################
# Find number of days until code occurring
################################################################################

matching <- getMatchingCodeSets(preds, serial_to_codes, date_field=date_field, sc_code_days=.Machine$integer.max, parallel=FALSE)
preds$all <- matching

code_dates <- list()
first_code_dates <- list()
first_codes <- list()
for(r in splitDataFrame(preds)) {
  cs <- r$all[[1]]
  code_date <- NA
  first_code <- NA
  first_code_date <- NA
  if(length(cs)!=0) {
    cs <- sortBy(cs, function(c) c$OCCUR_DATE)
    first_code <- codeToLabel(cs[[1]])
    first_code_date <- cs[[1]]$OCCUR_DATE
    for(c in cs) {
      if(codeToLabel(c)==r$Label) {
        code_date <- c$OCCUR_DATE
      }
    }
  }
  code_dates <- append(code_dates, code_date)
  first_codes <- append(first_codes, first_code)
  first_code_dates <- append(first_code_dates, first_code_date)
}
# Drop list of code objects
preds <- preds[,names(preds)!='all']
preds$CodeDate <- unlist(code_dates)
preds$Elapsed <- preds$CodeDate - preds$GetDate
preds$FirstCode <- unlist(first_codes)
preds$FirstCodeDate <- unlist(first_code_dates)
preds$FirstCodeElapsed <- preds$FirstCodeDate - preds$GetDate

print("All predictions:")
print(preds)


print("Stats for predictions with 30 days of code data:")
latestDate <- latestFileDate()
preds_all <- preds
preds <- preds[preds_all$GetDate < latestDate-30,]

h <- groupBy(splitDataFrame(preds), function(r) r$Label)
counts <- mapHash(h, length)
counts[["overall"]] <- nrow(preds)

getHitRate <- function(preds, field="CodeDate", cutoff_days=30) {
  if(length(preds) == 0) {
    return(NA)
  } else {
    elapsed <- preds[,field] - preds$GetDate
    res <- sum(elapsed <= cutoff_days & !is.na(elapsed)) / nrow(preds)
    return(res)
  }
}

hitrates <- mapHash(h, function(x) x %>% bind_rows %>% getHitRate)
hitrates[["overall"]] <- getHitRate(preds)
hitrates_any <- mapHash(h, function(x) x %>% bind_rows %>% getHitRate(field="FirstCodeDate"))
hitrates_any[["overall"]] <- getHitRate(preds, field="FirstCodeDate")

print("Counts:")
print(counts)
print("Hitrates:")
print(hitrates)
print("Hitrates - any code:")
print(hitrates_any)

# Plot distribution of predicton to code occurring
hist(as.integer(preds$FirstCodeElapsed), breaks=31)
#hist(as.integer(preds[preds$Label=="X492_0","FirstCodeElapsed"]), breaks=31)
#hist(as.integer(preds[preds$Label=="X899_0","FirstCodeElapsed"]), breaks=31)
