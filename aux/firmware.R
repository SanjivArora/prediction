
# Examine effect of firmware updates on service codes
# For each firmware module, find the highest firmware version present in the fleet as of that day (per model)
# Flag each module+machine pair as up to date or not up to date depending on whether the firmware version equals latest
# TODO: Exclude package_all



source("common.R")

require(dplyr)
require(memoise)
require(mlr)
require(itertools)
require(forcats)
require(magrittr)
require(plotly)
require(purrr)


data_days <- 100
data_days <- 365


file_sets <- getEligibleFileSets(regions, device_models, sources, days=data_days, end_date=end_date)
data_files <- unlist(file_sets)
predictors_all <- dataFilesToDataset(
  data_files,
  sources,
  sample_rate,
  label_days,
  delta_days=delta_days,
  deltas=deltas,
  only_deltas=only_deltas,
  features=fs,
  parallel=parallel
)

service_codes <- readCodes(regions, device_models, target_codes, days=data_days, end_date=end_date, parallel=parallel)
serial_to_service_codes <- makeSerialToCodes(service_codes)



preds_unique <- predictors_all %>% dplyr::distinct(Serial, RetrievedDateTime, .keep_all=T)
preds_processed <- preds_unique %>% processRomVer
preds_processed$RetrievedDate <- preds_processed[,'GetDate.x']
idxs <- grepl('RomVer_VER_[^.]*$', names(preds_processed), perl=T)
fw <- preds_processed[,idxs]
# Find firmware with at least two unique versions
fw_value_counts <- sapply(fw, . %>% table %>% length)
fw_names <- names(fw_value_counts[fw_value_counts > 1])

flag_fw <- function(p, key) {
  fw <- p[,fw_names]
  for (name in names(fw)) {
    fw_max <- max(fw[,name], na.rm=T)
    p[,paste(name, '.latest', sep='')] <- fw_max
    p[,paste(name, '.current', sep='')] <- fw[,name]==fw_max
  }
  # fw_max <- apply(fw, 2, function(x) max(x, na.rm=T))
  # #print(fw_max[grepl('.*ADF.*', names(fw_max))])
  # p[,paste(fw_names, '.latest', sep='')] <- fw_max
  # p[,paste(fw_names, '.current', sep='')] <- fw==fw_max
  return(p)
}

fw_fields <- append(list('Serial', 'RetrievedDate', 'RetrievedDateTime', 'Model'), fw_names) %>% unlist
per_day <- preds_processed %>% dplyr::group_by(RetrievedDate, Model)
res <- per_day[,fw_fields] %>% group_modify(flag_fw)
res <- res %>% bind_rows %>% arrange(desc(RetrievedDateTime))
#View(names(res))
# selection <- grepl('.*ADF.*', names(res))
# res[,"RomVer.RomVer_VER_ADF"] %>% table
# res[,"RomVer.RomVer_VER_ADF.latest"] %>% table
# res[,"RomVer.RomVer_VER_ADF.current"] %>% table

selection <- grepl('.*.current', names(res))
sapply(res[,selection], . %>% table)

sc_subcodes <- service_codes[,c('SC_CD', 'SC_SERIAL_CD')]
service_codes$label <- paste(service_codes$SC_CD, service_codes$SC_SERIAL_CD, sep='_')
freqs <- table(service_codes$label)
sc_labels <- freqs[freqs > 200] %>% names
sc <- service_codes[service_codes$label %in% sc_labels,]
res$had_sc <- FALSE
labels <- paste('n_sc', sc$label %>% unique, sep='')
for (l in labels) {
  res[,l] <- 0
}
by_ser <- dplyr::group_by(res, Serial)
by_ser %<>% arrange(desc(RetrievedDateTime))
# Count codes for each reading
process_ser <- function(df, key) {
  #x<-df[,names(df)]
  df[,'RetrievedDate.delta'] <- df$RetrievedDate - shift(df$RetrievedDate, 1, type='lead')
  df[,'RetrievedDateTime.delta'] <- df$RetrievedDateTime - shift(df$RetrievedDateTime, 1, type='lead')
  ser <- key$Serial
  cs <- serial_to_service_codes[[ser]]
  for (c in cs) {
    if(c$label %in% sc_labels) {
      c_datetime <- ymd(c$OCCUR_DATE) + hms(c$OCCUR_TIME)
      # Find first index where code matches, ignore if older than oldest reading
      if(c_datetime <= last(df$RetrievedDateTime)) {
        next
      }
      idx <- detect_index(df$RetrievedDateTime, function(t) t >= c_datetime, .right=T)
      f <- paste('n_sc', c$label, sep='')
      if(idx!=0) {
        df[idx,f] <- df[idx,f] + 1
      } else {
        print("Unmatched sc time for:")
        print(c)
      }
    }
  }
  df
}
res <- by_ser %>% group_modify(process_ser) %>% bind_rows

for (l in sc$label %>% unique) {
  cs <- sc[sc$label==l,]
  fields <- c(paste('n_sc', cs$label %>% unique, sep=''))
  res[(res$Serial %in% (cs$Serial %>% unique)) & (res$RetrievedDate %in% cs$OCCUR_DATE %>% unique), fields] <- TRUE
}

# calcResults <- function(name) {
#   rs <- list()
#   for (l in labels) {
#     stats <- res[,c(name, l)] %>% table %>% data.frame
#     cur <- stats[stats[,name]==T,]
#     old <- stats[stats[,name]==F,]
#     to_frac <- function(x) {
#       truth_vals <- x[,l] %>% unique
#       if (length(truth_vals) == 1) {
#         v <- as.integer(truth_vals)
#       } else {
#         v <- x[x[,l]==T,]$Freq / sum(x$Freq)
#       }
#       # Rate per thousand
#       v * 1000
#     }
#     if(nrow(stats) != 4) {
#       # Not enough distinct values
#       #return("Not enough distinct values")
#     } else {
#       r <- data.frame(
#         "var"=name, "SC"=l,
#         "Current"=to_frac(cur),
#         "Old"=to_frac(old),
#         'n_cur'=sum(cur$Freq),
#         'n_old'=sum(old$Freq),
#         'n_code_total'=stats[stats[,l]==T,]$Freq %<>% sum
#       )
#       rs %<>% append(list(r))
#     }
#   }
#   bindRowsForgiving(rs)
# }
deltaToDays <- function(timedelta) {
  (timedelta %>% unlist %>% as.numeric) / (3600 * 24)
}

calcResults <- function(name) {
  print(name)
  rs <- list()
  for (l in labels) {
    cur <- res[res[,name]==T,]
    old <- res[res[,name]==F,]
    to_frac <- function(x) {
      if(nrow(x)==0) {
        v <- NA
      } else {
        days <- x[,'RetrievedDateTime.delta'] %>% deltaToDays
        # Codes per day
        v <- sum(x[,l], na.rm=T) / sum(days, na.rm=T)
      }
      # Rate per thousand machine-days
      v * 1000
    }
    r <- data.frame(
      "var"=name, "sc"=l,
      "current"=to_frac(cur),
      "old"=to_frac(old),
      'machine_days_cur'=cur[,'RetrievedDateTime.delta'] %>% deltaToDays %>% sum(na.rm=T),
      'machine_days_old'=old[,'RetrievedDateTime.delta'] %>% deltaToDays %>% sum(na.rm=T),
      'n_code_total'=res[,l] %>% sum(na.rm=T)
    )
    rs %<>% append(list(r))
  }
  bindRowsForgiving(rs)
}
selection <- grepl('.*.current', names(res))
selection <- names(res)[selection]
x <- lapply(selection, calcResults)
# Disable scientific notation for displaying results
options(scipen=99)
x

