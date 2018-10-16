
require(data.table)
require(memoise)

only_numeric <- function(df) {
  temp <- df[,sapply(df, is.numeric)]
  #temp <- temp[,-c(grep("Date",names(temp),ignore.case = TRUE),grep("Time",names(temp),ignore.case = TRUE),grep("EDP",names(temp),ignore.case = TRUE))]
  return (temp)
}


read_data <- function(date, models, data_days, sources, features=c()) {
  dfs <- dataframes_for_models(date, models, days=data_days, sources=sources)
  aug <- augment_with_deltas(dfs)
  # Select features, default to all
  all_names <- names(aug[[1]])
  fs <- get_feature_names(features, all_names)
  #fs <- unlist(features)
  aug <- lapply(aug, function(df) {df[,fs]})
  
  numeric_all <- lapply(aug, only_numeric)
  return(numeric_all)
}
#read_data <- memoise(read_data)


read_sc <- function(date, models, days, offset) {
  sc_frames = dataframes_for_models(date, models, days=days, sources=c("SC"))
  SC_codes <- process_datasets(sc_frames)
  
  selected_SC <- filter(
    SC_codes,
    SC_codes$Code >= 200 && SC_codes$code <300 |
      SC_codes$Code >= 600 && SC_codes$code <700 |
      SC_codes$Code >= 800
  )
  selected_SC <- filter(selected_SC, !is.na(Serial))
  selected_SC <- filter(selected_SC, !is.na(Date))
  oldest_target_code <- date - (days - 1)
  selected_SC <- filter(selected_SC, as.Date(selected_SC$Date) >= as.Date(oldest_target_code))
  
  # Final SC Dataset
  index_to_code <- get_index_to_code(distinct(selected_SC), offset)
  return(index_to_code)
}
#read_sc <- memoize(read_sc)