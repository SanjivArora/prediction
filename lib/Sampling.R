require(hash)
require(dplyr)
require(testit)

source("lib/Util.R")
source("lib/Parallel.R")
source("lib/Logging.R")
source("lib/SCFile.R")

# R's idiosyncratic handling of namespaces / environments requires that we don't do the obvious thing and call this "log", since the reference would be overridden elsewhere.
sampling_log <- getModuleLogger("Sampling")

# Return groups of daily predictor data files where all required sources are present for a model on a given day. Sort these by date.
getFileSets <- function(data_files, required_sources) {
  group_hash <- groupBy(data_files, function(f) list(f$date, f$region, f$model))
  res <- list()
  for(fs in values(group_hash, simplify=FALSE)) {
    matches <- filterBy(fs, function(f) f$source %in% required_sources)
    if(length(matches) == length(required_sources)) {
      res <- append(res, list(matches))
    }
  }
  res <- sortBy(res, function(fs) fs[[1]]$date)
  return(res)
}

#getFileSets(fs, c("Count", "PMCount"))

# Return list containing valid sets of data files for each day (i.e. top level lest containing a list per day with data, these lists contain sets of DataFile objects grouped by {region, model}). Top level list is sorted by date.
getDailyFileSets <- function(...) {
  h <- getDailyFileSetsHash(...)
  res <- sortBy(h, function (fs) fs[[1]][[1]]$date)
  return(res)
}

getDailyFileSetsHash <- function(data_files, required_sources) {
  file_sets <- getFileSets(data_files, required_sources)
  h <- groupBy(file_sets, function (fs) fs[[1]]$date)
  return(h)
}

InstanceCounter <- setRefClass(
  "InstanceCounter",
  fields = list(
    ###################
    # Private variables
    ###################
    # Hash mapping serial to SC code rows
    serial_to_codes = "hash",
    # Look ahead 0-n days to check for code
    sc_days = "numeric",
    # Minimum number of SC code instance days for code to register
    min_count = "numeric",
    # Target total sample count (approximate)
    n_target_total = "numeric",
    # Target fraction of positive observations if upsampling is enabled (approximate, for sampling)
    n_target_positive_fraction = "numeric",
    # Hash mapping code number to count. "0" is a special key for control case instances (i.e. no SC codes).
    counts = "hash",
    # Total number of predictor rows
    n_total = "numeric",
    # If set, cap sampling frequency at this amount when upsampling is enabled
    cap_freq = "numeric",
    # If set, upsample positive cases per value of n_target_positive_fraction
    upsample_positive = "logical"
  ),
  methods = list(
    initialize = function(serial_to_codes, sc_days=14, min_count=10, n_target_total=2000, n_target_positive_fraction=0.5, cap_freq=1, upsample_positive=TRUE) {
      callSuper()
      .self$serial_to_codes <- serial_to_codes
      .self$sc_days <- sc_days
      .self$min_count <- min_count
      #code_list <- splitDataFrame(.self$codes)
      #.self$serial_to_codes <- groupBy(code_list, function (c) c$Serial)
      .self$counts <- hash()
      .self$counts[["0"]] <- 0
      .self$n_total <- 0
      .self$n_target_total <- n_target_total
      .self$n_target_positive_fraction <- n_target_positive_fraction
      .self$cap_freq <- cap_freq
      .self$upsample_positive <- upsample_positive
    },
    # Accept a dataframe for predictor data for the day - expects to be called in order but OK to skip calls for days with no data
    processDay = function(day_data, date) {
      sampling_log$debug(paste("Getting counts for", date))
      serials <- day_data$Serial
      # TODO: Would be marginally better to use per-row GetDate rather than data file date
      for(serial in serials) {
        cs <- getWithDefault(.self$getSerialToCodes(), serial, list())
        hits <- getMatchingCodes(cs, date, .self$sc_days)
        hit_codes <- unique(lapply(hits, function(c) codeToLabel(c)))
        for(c in hit_codes) {
          count <- getWithDefault(.self$counts, c, 0)
          .self$counts[[c]] <- count+1
        }
        # If no hits, add to count for control cases
        if(length(hits) == 0) {
          count <- getWithDefault(.self$counts, "0", 0)
          .self$counts[["0"]] <- count+1
        }
        .self$n_total <- .self$n_total + 1
      }
    },
    getEligibleCounts = function() {
      filterBy(.self$getCounts(), function(count) count >= .self$min_count)
    },
    getCounts = function() {
      .self$counts
    },
    getTotal = function() .self$n_total,
    getNegative = function() .self$negative,
    setMinCount = function(x) .self$min_count <- x,
    # Get sampling frequencies for SC codes such that we have approximately equal representation for each code.
    # Return a hash mapping SC code to frequency
    # This ignores interactions between codes, so the frequency will be overestimated if observations have multiple codes.
    # As codes are expected to have low base rates this is usually fine for the intended use and allows a simple implementation.
    getSamplingFrequencies = function(disable_sampling_cap=FALSE) {
      n_target_control <- .self$n_target_total * (1 - .self$n_target_positive_fraction)
      if(.self$upsample_positive) {
        cs <- .self$getEligibleCounts()
        if(length(cs)==0) {
          return(hash())
        }
        # Don't count the control cases
        target <- .self$n_target_total * .self$n_target_positive_fraction / (length(cs) - 1)
        freqs <- hash()
        for(code in keys(cs)) {
          if(code=="0") {
            freqs[["0"]] <- n_target_control  / cs[["0"]]
          } else {
            freqs[[code]] <- target / cs[[code]]
          }
        }
      } else {
        cs <- .self$getCounts()
        base_freqs <- .self$getFrequencies()
        if(length(cs)==0) {
          return(hash())
        }
        freqs <- hash()
        for(code in keys(cs)) {
          freqs[[code]] <- (.self$n_target_total / .self$n_total) * base_freqs[[code]]
        }
      }

      # If cap_freq is set, cap sampling frequencies to this value
      res <- freqs
      if(!disable_sampling_cap && !is.na(.self$cap_freq)) {
        res <- hash()
        for(code in keys(freqs)) {
          frq <- freqs[[code]]
          res[[code]] <- min(frq, .self$cap_freq)
        }
      }
      return(res)
    },
    # Get frequencies for each code, including control cases
    getFrequencies = function() {
      cs <- .self$getCounts()
      res <- hash()
      if(length(cs)==0) {
        return(res)
      }
      for(code in keys(cs)) {
          res[[code]] <- cs[[code]] / .self$n_total
      }
      return(res)
    },
    getTargetFrequency = function() {.self$getTargetTotal() / .self$getTotal()},
    getControlCount = function() {
      .self$counts[["0"]]
    },
    getSerialToCodes = function() {
      .self$serial_to_codes
    },
    getSerialToEligibleCodes = function() {
      eligible_counts <- .self$getEligibleCounts()
      mapHash(
        .self$getSerialToCodes(),
        function(cs) {
          filterBy(
            cs,
            function(c) has.key(codeToLabel(c), eligible_counts)
          )
        }
      )
    },
    setTargetPositiveFraction = function(x) {
      .self$n_target_positive_fraction <- x
    },
    getTargetPositiveFraction = function() {.self$n_target_positive_fraction},
    setTargetTotal = function(x) {
      .self$n_target_total <- x
    },
    getTargetTotal = function() {.self$n_target_total},
    # Merge another counter instance (for parallelization)
    mergeCounts = function(other) {
      .self$counts <- addHash(.self$counts, other$counts)
      .self$n_total <- .self$n_total + other$n_total
    },
    getSCDays = function() {.self$sc_days},
    setUpsamplePositive = function(x) {.self$upsample_positive <- x},
    getUpsamplePositive = function() {.self$upsample_positive}
  )
)

makeSerialToCodes <- function(codes) {
  code_list <- splitDataFrame(codes)
  res <- groupBy(code_list, function (c) c$Serial)
  return(res)
}

# Work around restrictions of reference classes so we don't have to store a redundant codes dataframe on counter object
makeInstanceCounter <- function(codes, ...) {
  serial_to_codes <- makeSerialToCodes(codes)
  res <- InstanceCounter(serial_to_codes=serial_to_codes, ...)
  return(res)
}

dailyFileSetToDataframe <- function(daily_file_set, features=features) {
  print(daily_file_set)
  parts <- lapply(daily_file_set, function(fs) dataFilesToDataframe(fs, features=features))
  df <- bindRowsForgiving(parts)
  # Set row names to Serial
  row.names(df) <- unlist(df[,'Serial'])
  return(df)
}

# Call function for each day with the date and a hash mapping date strings to valid datafile sets. Run this in parallel over models and sources.
visitPredictorDataframes <- function(data_files, required_sources, f, features=FALSE, parallel=FALSE, ncores=NA) {
  daily_file_sets <- getDailyFileSetsHash(data_files, required_sources)
  sampling_log$debug(paste("Visiting", length(daily_file_sets), "daily data frames"))
  # Run garbage collection to minimize duplicated work in children
  gc(verbose=FALSE, full=TRUE)
  visit <- function(date_string) {
    file_sets <- daily_file_sets[[date_string]]
    print(date_string)
    print(file_sets)
    date <- as.Date(date_string)
    df <- dailyFileSetToDataframe(file_sets, features=features)
    res <- f(df, date, daily_file_sets)
    return(res)
  }
  res <- plapply(keys(daily_file_sets), visit, parallel=parallel, ncores=ncores)
  assert(length(keys(daily_file_sets))==length(res))
  sampling_log$debug("Finished visiting dataframes")
  return(res)
}

dataFilesToCounter <- function(data_files,
                               required_sources,
                               sc_codes,
                               sc_days=14,
                               min_count=10,
                               cap_sampling=TRUE,
                               features=FALSE,
                               parallel=TRUE) {
  if(cap_sampling) {
    cap_freq <- 1
  } else {
    cap_freq <- NA
  }
  counter <- makeInstanceCounter(codes=sc_codes, sc_days=sc_days, min_count=min_count, cap_freq=cap_freq)
  # Dirty trick for efficient parallelization - if executing in parallel, update the counter with count hashes from children.
  # Since the counter environment in this process will not be updated in parallel execution, this works.
  # If we are executing serially, the counter will automatically update.
  counters <- visitPredictorDataframes(
    data_files,
    required_sources,
    function(df, date, daily_file_sets) {
      # Run garbage collection to get rid of old counter objects
      gc(verbose=FALSE, full=TRUE)
      counter <- makeInstanceCounter(codes=sc_codes, sc_days=sc_days, min_count=min_count, cap_freq=cap_freq)
      counter$processDay(df, date)
      return(counter)
    },
    features=features,
    parallel=parallel
  )
  if(parallel) {
    for(c in counters) {
      counter$mergeCounts(c)
    }
  }
  dropped_counts <- subtractHash(counter$getCounts(), counter$getEligibleCounts())
  if(length(dropped_counts) > 0) {
    sampling_log$info(
      paste(length(dropped_counts), "counts for SC codes under the eligibility threshold of", counter$min_count, "instances")
    )
  }
  # Always one control count
  if(length(counter$getEligibleCounts()) == 1) {
    sampling_log$warn("No eligible counts for SC codes")
  }
  if(counter$getControlCount()==0) {
    sampling_log$warn("No control case instances")
  }
  return(counter)
}


getDeltaDataFrames <- function(date, daily_file_sets, delta_days) {
  prior_file_sets <- lapply(delta_days, function(n) getWithDefault(daily_file_sets, as.character(date-n), list()))
  if (any(unlist(lapply(prior_file_sets, function(fs) length(fs)==0)))) {
    sampling_log$info(paste("Missing required previous data to generate deltas for", date, "- skipping"))
    return(FALSE)
  }
  prior_dfs <- lapply(prior_file_sets, dailyFileSetsToDataframe)
  if (any(unlist(lapply(prior_dfs, function(df) !is.data.frame(df))))) {
    return(FALSE)
  }
  return(prior_dfs)
}

# Take a dataframe, a list of previous dataframes, and a matching list of deltas.
# Return a dataframe containing serials present in all dataframes, augmented with deltas from the current dataframe.
augmentWithDeltas <- function(df, date, previous, delta_days, only_deltas=FALSE) {
  assert(is.data.frame(df))
  prior_serials_parts <- lapply(previous, function(d) d[,"Serial"])
  prior_serials <- Reduce(append, prior_serials_parts)
  # User serials present in all dataframes
  valid_serials <- intersect(prior_serials, unlist(df$Serial))
  n_dropped <- length(df$Serial) - length(valid_serials)
  if(length(valid_serials) == 0) {
    sampling_log$info(paste("No valid serials for", date, "- skipping"))
    return(FALSE)
  }
  if(n_dropped > 0) {
    sampling_log$debug(paste("Dropped", n_dropped, "records for", date, "due to serials not being present in previous dataframes required to generate deltas"))
  }
  valid <- df[valid_serials,]
  num_col_vector <- unlist(lapply(valid, is.numeric))
  cur <- valid[,num_col_vector]
  # Use column names with previous data in case of order change or added/removed columns
  num_cols <- colnames(valid)[num_col_vector]
  # Drop non-delta numerical data if enabled
  if(only_deltas) {
    valid <- valid[,!num_col_vector]
  }
  parts <- list(valid)
  for(i in 1:length(delta_days)) {
    prev_df <- previous[[i]]
    assert(is.data.frame(prev_df))
    n_delta <- delta_days[[i]]
    prev <- prev_df[valid_serials, num_cols]
    non_num_cols <- colnames(prev)[unlist(lapply(prev, function(c) !is.numeric(c)))]
    if (length(non_num_cols)>0) {
      sampling_log$info(paste(length(non_num_cols),"columns that are numeric for", date, "but not for", date-n_delta))
      lapply(non_num_cols, sampling_log$debug)
      # Need a better long term solution, this coercion will introduce a lot of noise
      prev[,non_num_cols] <- lapply(prev[,non_num_cols], as.numeric)
    }
    deltas <- cur - prev
    colnames(deltas) <- paste(colnames(deltas), "delta", n_delta, sep=".")
    parts <- append(parts, list(deltas))
  }
  res <- Reduce(cbind, parts)
  assert(is.data.frame(res))
  return(res)
}

# Sample dataframe to 100% coverage before taking duplicates. Augment data with deltas to previous days for numeric values.
sampleDataFrame <- function(df, date, sample_rate, daily_file_sets, delta_days=c(1,3,7), deltas=TRUE, only_deltas=TRUE) {
  # TODO: currently if a serial has multiple SC codes, we sample independently for each code.
  # Combined sampling or adjusting frequencies to drop overlapped samples would improve efficiency and class balance.
  sampling_log$debug(paste("Sampling dataset for", date))
  n <- nrow(df) * sample_rate
  sampled_serials <- stochasticSelection(df$Serial, n)
  df <- df[unlist(sampled_serials),]
  if(deltas) {
    prior_dfs <- getDeltaDataFrames(date, daily_file_sets, delta_days)
    if(!is.list(prior_dfs)) {
      return(FALSE)
    }
    augmented <- augmentWithDeltas(df, date, prior_dfs, delta_days, only_deltas)
  } else {
    augmented <- df
  }
  if(!is.data.frame(augmented)) {
    return(FALSE)
  }
  # Filter to serials for which we have data 
  sampled_serials <- intersect(df$Serial, augmented$Serial)
  res <- augmented[unlist(sampled_serials),]
  assert(is.data.frame(res))
  return(res)
}

dataFilesToDataset <- function(data_files,
                               required_sources,
                               sc_codes,
                               sample_rate=1,
                               sc_days=14,
                               delta_days=c(1, 3, 7),
                               deltas=TRUE,
                               only_deltas=TRUE,
                               features=FALSE,
                               parallel=TRUE,
                               ncores=NA) {
  # This is a memory-intensive operation so limit parallelism
  if(identical(ncores, NA)) {
    ncores <- (max(1, detectCores() / 4))
  }
  parts <- visitPredictorDataframes(
    data_files,
    required_sources,
    function(df, date, daily_file_sets) {
      # Run garbage collection to free up memory for siblings
      gc(verbose=FALSE, full=TRUE)
      sampleDataFrame(df, date, sample_rate, daily_file_sets, delta_days, deltas, only_deltas)
    },
    features=features,
    parallel=parallel,
    ncores=ncores
  )
  # Filter out results for days without required data to generate deltas
  filtered_parts <- filterBy(parts, function(p) is.data.frame(p))
  total_rows_parts <- sum(unlist(lapply(filtered_parts, nrow)))
  res <- bindRowsForgiving(filtered_parts)
  total_rows <- nrow(res)
  if(total_rows != total_rows_parts) {
    print(paste("Dropped", total_rows - total_rows_parts, "rows when combining daily dataframes"))
  }
  return(res)
}