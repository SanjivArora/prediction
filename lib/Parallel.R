source("lib/Logging.R")

parallel_log <- getModuleLogger("Parallel")

ncores = detectCores() / 2

# Wrap parallel lapply implementation to allow easy debugging and change of backend
plapply <- function(l, f, parallel=TRUE) {
  if(parallel) {
    results <- mclapply(l, f, mc.cores=ncores, mc.cleanup=TRUE)
    have_errors <- filterBy(results, function(r) attributes(r)$class=="try-error")
    if(length(have_errors) > 0) {
      parallel_log$warn(paste(length(have_errors), "children have exceptions, rethrowing first exception"))
      e <- attributes(have_errors[[1]])$condition
      stop(e)
    }
  } else {
    results <- lapply(l, f)
  }
  return(results)
}