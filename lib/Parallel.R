require(parallel)

source("lib/Logging.R")

parallel_log <- getModuleLogger("Parallel")

# Default to number of physical cores, assuming 2-way hyperthreading
default_ncores = max(1, (detectCores() / 3) - 1)
default_ncores = ceiling(default_ncores)

# Wrap parallel lapply implementation to allow easy debugging and change of backend
plapply <- function(l, f, parallel=TRUE, ncores=NA) {
  if(parallel) {
    if(identical(ncores, NA)) {
      ncores <- default_ncores
    }
    results <- mclapply(l, f, mc.cores=ncores, mc.cleanup=TRUE)
    have_errors <- filterBy(results, function(r) attributes(r)$class=="try-error")
    if(length(have_errors) > 0) {
      parallel_log$warn(paste(length(have_errors), "exceptions from child processes, rethrowing first exception"))
      e <- attributes(have_errors[[1]])$condition
      stop(e)
    }
  } else {
    results <- lapply(l, f)
  }
  return(results)
}