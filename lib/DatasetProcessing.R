require(profvis)
require(data.table)
require(hash)
require(memoise)

#source("Dataset.R")


extract_serial <- function(index_string) {
  fin <- regexpr("^[^-]*", index_string)
  s <- substr(index_string, 1, attr(fin, "match.length"))
  return(s)
}


# Accept a hash mapping indices to list of individual row dataframes, return a single dataframe with labeled rows and median values
group_map_to_df <- function(h) {
  means_list <- list()
  means_list <- plapply(values(h), function(dfs) {
    #Reduce(`+`, dfs) / length(dfs)
    dfs[[1]]
  })
  res <- rbindlist(means_list)
  res <- as.data.frame(res)
  row.names(res) <- keys(h)
  return(res)
}


get_index_to_data <- function(all_dfs, indices, start_dates, delta_days, latest_delta, offset) {
  # Query for serials for each day
  
  serials <- lapply(indices, extract_serial)
  
  print("Iterating over days of data")
  parts <- plapply(1:delta_days, function(i) {
    part <- hash()
    print(paste("Processing", i, "of", delta_days))
    current_date <- latest_delta - lubridate::days(i-1)
    day_data <- all_dfs[[i]]
    
    # Build list of current indices
    current <- list()
    for(j in 1:length(indices)) {
      index <- indices[[j]]
      start_date <- start_dates[[j]]
      if(current_date >= start_date && current_date < start_date + window_days) {
        current <- append(current, list(index))
      }
    }
    
    # Build index -> [row] map
    for(j in 1:length(current)) {
      if(length(current)!=0) {
        index <- current[[j]]
        row <- day_data[j,]
        # Only add row if we have valid data
        if(!all(is.na(row))) {
          part[[index]] <- row
        }
      }
    }
    return(part)
  })
  
  print("Building list of values")
  values <- plapply(indices, function(index) {
    data <- list()
    for(part in parts) {
      if(has.key(index, part)[[1]]) {
        data <- append(data, list(part[[index]]))
      }
    }
    return(data)
  })

  # Only use indices that have data
  filtered_indices <- list()
  filtered_values <- list()
  # Expensive due to linear lookup cost, but probably fine
  index_to_data <- hash()
  for (i in 1:length(indices)) {
    index <- indices[[i]]
    value <- values[[i]]
    if(length(value)) {
      index_to_data[[index]] <- value
    }
  }
  return(index_to_data)
}
#get_index_to_data <- memoise(get_index_to_data, cache=fs_cache)

get_test_data <- function(all_dfs, index_to_code, window_days, delta_days, latest_delta, offset) {
  print("Processing test data")
  indices <- keys(index_to_code)
  start_dates <- list()
  serials <- list()
  
  for(index in indices) {
    code <- index_to_code[[index]]
    start_date <- response_start_date(code$Date, offset)
    serial <- as.character(code$Serial)
    start_dates <- append(start_dates, list(start_date))
    serials <- append(serials, list(serial))
  }
  
  index_to_control_data <- get_index_to_data(all_dfs, indices, start_dates, delta_days, latest_delta, offset)
  print("Restructuring test data")
  data <- group_map_to_df(index_to_control_data)
  return(data)
}


get_control_data <- function(all_dfs, index_to_code, window_days, delta_days, latest_delta, offset, size_multiple=NULL) {
  print("Processing control data")
  test_serials<-lapply(keys(index_to_code), extract_serial)
  # TODO: cleaner way to get list of all serials that accounts for missing days of data
  all_serials <- row.names(all_dfs[[1]])
  serials <- setdiff(all_serials, test_serials)
  if(!is.null(size_multiple)) {
    # Limit number of controls to test group size to limit processing time and class imbalance
    test_size <- length(keys(index_to_code))
    n_controls <- min(length(serials), size_multiple * test_size)
    serials <- sample(serials, n_controls, replace=TRUE)
  } else {
    n_controls <- length(serials)
  }
  oldest_delta <- latest_delta - (delta_days - 1)
  start_dates <- sample(seq(oldest_delta, latest_delta, by="day"), n_controls, replace=TRUE)
  index_to_control_data <- get_index_to_data(all_dfs, serials, start_dates, delta_days, latest_delta, offset)
  print("Restructuring control data")
  data <- group_map_to_df(index_to_control_data)
  return(data)
}


get_dataset <- function(date, all_dfs, index_to_code, window_days, delta_days, offset, control_size_multiple=NULL, replace_na_vals=TRUE) {
  if(nrow(all_dfs[[1]]) == 0) {
    warning("Empty dataset")
  }
  data_test_group <- get_test_data(all_dfs, index_to_code, window_days, delta_days, date, offset)
  if(nrow(data_test_group) == 0) {
    warning("No test cases")
  }
  data_control_group <- get_control_data(all_dfs, index_to_code, window_days, delta_days, date, offset, control_size_multiple)
  if(nrow(data_control_group) == 0) {
    warning("No control cases")
  }
  dataset <- rbind(data_test_group, data_control_group)
  if(replace_na_vals) {
  dataset <- replace_na(dataset)
  }
  return(dataset)
}
#get_dataset <- memoise(get_dataset, cache=fs_cache)
