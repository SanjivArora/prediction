require(hash)
require(dplyr)

source("lib/Util.R")
source("lib/Parallel.R")
source("lib/Logging.R")

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
getDailyFileSets <- function(data_files, required_sources) {
  file_sets <- getFileSets(data_files, required_sources)
  h <- groupBy(file_sets, function (fs) fs[[1]]$date)
  res <- sortBy(h, function (fs) fs[[1]][[1]]$date)
  return(res)
}

getMatchingCodes <- function(codes, date, sc_days) {
  if(class(codes) == "data.frame") {
    cs <- splitDataFrame(codes)
  } else {
    cs <- codes
  }
  res <- filterBy(cs, function(c) {
    delta <- c$OCCUR_DATE - date
    in_window <- delta >= 0 && delta <= sc_days
    return(in_window)
  })
  return(res)
}

InstanceCounter <- setRefClass(
  "InstanceCounter",
  fields = list(
    ###################
    # Private variables
    ###################
    # Dataframe of code instances
    codes = "data.frame",
    # Look ahead 0-n days to check for code
    sc_days = "numeric",
    # Minimum number of SC code instance days for code to register
    min_count = "numeric",
    # Target total number of SC codes (approximate, for sampling)
    n_target_sc = "numeric",
    # Target total number of control cases (approximate, for sampling)
    n_target_control = "numeric",
    # Hash mapping serial to SC code rows
    serial_to_codes = "hash",
    # Hash mapping code number to count. "0" is a special key for control case instances (i.e. no SC codes).
    counts = "hash",
    # Total number of predictor rows
    n_total = "numeric"
  ),
  methods = list(
    initialize = function(..., sc_days=14, min_count=10, n_target_sc=1000, n_target_control=1000) {
      callSuper(...)
      .self$sc_days <- sc_days
      .self$min_count <- min_count
      code_list <- splitDataFrame(.self$codes)
      .self$serial_to_codes <- groupBy(code_list, function (c) c$Serial)
      .self$counts <- hash()
      .self$counts[["0"]] <- 0
      .self$n_total <- 0
      .self$n_target_sc <- n_target_sc
      .self$n_target_control <- n_target_control
    },
    # Accept a dataframe for predictor data for the day - expects to be called in order but OK to skip calls for days with no data
    processDay = function(day_data, date) {
      sampling_log$debug(paste("Getting counts for", date))
      serials <- day_data$Serial
      # TODO: Would be marginally better to use per-row GetDate rather than data file date
      for(serial in serials) {
        cs <- getWithDefault(.self$getSerialToCodes(), serial, list())
        hits <- getMatchingCodes(cs, date, .self$sc_days)
        hit_codes <- unique(lapply(hits, function(c) c$SC_CD))
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
    getCounts = function() {
      filterBy(.self$getRawCounts(), function(count) count >= .self$min_count)
    },
    getRawCounts = function() {
      .self$counts
    },
    getTotal = function() .self$n_total,
    getNegative = function() .self$negative,
    setMinCount = function(x) .self$min_count <- x,
    # Get sampling frequencies for SC codes sucht that we have approximately equal representation for each code.
    # Return a hash mapping SC code to frequency
    getSamplingFrequencies = function() {
      cs <- .self$getCounts()
      if(length(cs)==0) {
        return(hash())
      }
      # Don't count the control cases
      target <- .self$n_target_sc / (length(cs) - 1)
      res <- hash()
      for(code in keys(cs)) {
        if(code=="0") {
          res[["0"]] <- .self$n_target_control  / cs[["0"]]
        } else {
          res[[code]] <- target / cs[[code]]
        }
      }
      return(res)
    },
    getControlCount = function() {
      .self$counts[["0"]]
    },
    getSerialToCodes = function() {
      .self$serial_to_codes
    },
    setTargetSC = function(x) {
      .self$n_target_sc <- x
    },
    setTargetControl = function(x) {
      .self$n_target_control <- x
    },
    # Merge another counter instance (for parallelization)
    mergeCounts = function(other) {
      .self$counts <- addHash(.self$counts, other$counts)
      .self$n_total <- .self$n_total + other$n_total
    },
    getSCDays = function() {.self$sc_days}
  )
)

# Call function for each day with valid data, pass a dataframe containing merged predictor data for that day, and the date. Run this in parallel over models and sources.
visitPredictorDataframes <- function(data_files, required_sources, f, parallel=FALSE) {
  daily_file_sets <- getDailyFileSets(data_files, required_sources)
  sampling_log$debug(paste("Visiting", length(daily_file_sets), "daily data frames"))
  visit <- function(file_sets) {
    date <- file_sets[[1]][[1]]$date
    parts <- plapply(file_sets, function(fs) dataFilesToDataframe(fs), parallel=parallel)
    df <- bindRowsForgiving(parts)
    # Set row names to Serial
    row.names(df) <- unlist(df[,'Serial'])
    res <- f(df, date)
    return(res)
  }
  res <- plapply(daily_file_sets, visit, parallel=parallel)
  stopifnot(length(daily_file_sets)==length(res))
  return(res)
}

dataFilesToCounter <- function(data_files, required_sources, sc_codes, sc_days=14, min_count=10, parallel=TRUE) {
  counter <- InstanceCounter(codes=sc_codes, sc_days=sc_days, min_count=min_count)
  # Dirty trick for efficient parallelization - if executing in parallel, update the counter with count hashes from children.
  # Since the counter environment in this process will not be updated in parallel execution, this works.
  # If we are executing serially, the counter will automatically update.
  counters <- visitPredictorDataframes(
    data_files,
    required_sources,
    function(df, date) {
      counter <- InstanceCounter(codes=sc_codes, sc_days=sc_days, min_count=min_count)
      counter$processDay(df, date)
      return(counter)
    },
    parallel=parallel
  )
  if(parallel) {
    for(c in counters) {
      counter$mergeCounts(c)
    }
  }
  dropped_counts <- subtractHash(counter$getRawCounts(), counter$getCounts())
  if(length(dropped_counts) > 0) {
    sampling_log$info(
      paste("Dropped", length(dropped_counts), "counts for SC codes under the threshold of", counter$min_count, "instances")
    )
  }
  # Always one control count
  if(length(counter$getCounts()) == 1) {
    sampling_log$warn("No eligible counts for SC codes")
  }
  if(counter$getControlCount()==0) {
    sampling_log$warn("No control case instances")
  }
  return(counter)
}

# Sample dataframe to 100% coverage before taking duplicates
sampleDataFrame <- function(df, date, counts) {
  # TODO: currently if a serial has multiple SC codes, we sample independently for each code.
  # Combined sampling or adjusting frequencies to drop overlapped samples would improve efficiency and class balance.
  sampling_log$debug(paste("Sampling dataset for", date))
  freqs <- counts$getSamplingFrequencies()
  serial_to_codes = counts$getSerialToCodes()
  code_to_serials <- hash()
  for(serial in df$Serial) {
    cs <- getWithDefault(serial_to_codes, serial, list())
    hits <- getMatchingCodes(cs, date, counts$sc_days)
    hit_codes <- unique(lapply(hits, function(c) c$SC_CD))
    for (c in hit_codes) {
      current <- getWithDefault(code_to_serials, c, list())
      code_to_serials[[c]] <- append(current, serial)
    }
    # If no hits, add to control cases
    if(length(hits) == 0) {
      current <- getWithDefault(code_to_serials, "0", list())
      code_to_serials[["0"]] <- append(current, serial)
    }
  }
  sampled_serials <- list()
  for(code in keys(code_to_serials)) {
    serials <- code_to_serials[[code]]
    n <- getWithDefault(freqs, code, 0) * length(serials)
    sampled <- stochasticSelection(serials, n)
    sampled_serials <- append(sampled_serials, sampled)
  }
  n <- length(sampled_serials)
  res <- df[unlist(sampled_serials),]
  stopifnot(nrow(res)==n)
  return(res)
}

dataFilesToDataset <- function(data_files, required_sources, sc_codes, counts, sc_days=14, parallel=TRUE) {
  parts <- visitPredictorDataframes(
    data_files,
    required_sources,
    function(df, date) sampleDataFrame(df, date, counts),
    parallel=parallel
  )
  total_rows_parts <- sum(unlist(lapply(parts, nrow)))
  res <- bindRowsForgiving(parts)
  total_rows <- nrow(res)
  if(total_rows != total_rows_parts) {
    print(paste("Dropped", total_rows - total_rows_parts, "rows when combining daily dataframes"))
  }
  return(res)
}