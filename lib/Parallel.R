require(parallel)
require(httr)
require(curl)

# https://github.com/jeroen/unix
require(unix)

source("lib/Logging.R")

parallel_log <- getModuleLogger("Parallel")

# Default to number of physical cores, assuming 2-way hyperthreading
default_ncores = max(1, (detectCores() / 2) - 1)
default_ncores = ceiling(default_ncores)

# Set generous stack size (small stacks break modified CURL fetch)
rlimit_stack(2**25)

# Monkey patch curl_fetch_memory to prevent connection reuse
# Note: the following will break calling code that doesn't specify a handle
# This is to prevent the possiblity of child processes contending
# for the same connection.
# Additionally, disable SSL verification (apparently the SSL library also has issues with forking)
# This is reasonably safe for the intended use (same-region S3 on trusted infrastructure)
require(curl)
fetch_memory_patched <- {if(exists('fetch_memory_patched')) fetch_memory_patched else FALSE}
if(!fetch_memory_patched) {
  parallel_log$info("Patching curl_fetch_memory")
  orig_fetch <- getFromNamespace('curl_fetch_memory', 'curl')
  noreuse_fetch <- function(url, handle) {
    handle_setopt(handle, forbid_reuse=TRUE, ssl_verifyhost=FALSE, ssl_verifypeer=FALSE)
    res <- orig_fetch(url, handle)
    return(res)
  }
  assignInNamespace('curl_fetch_memory', noreuse_fetch, 'curl')
  fetch_memory_patched <<- TRUE
}

# Wrap parallel lapply implementation to allow easy debugging and change of backend
# If purge_curl is true, then inject a new empty httr CURL connection pool
# This is to work around the possiblity of child processes resuing the same handles
plapply <- function(l, f, parallel=TRUE, ncores=NA, purge_curl=TRUE, preschedule=TRUE) {
  if(parallel) {
    if(identical(ncores, NA)) {
      ncores <- default_ncores
    }
    wrapped_f <- function(...) {
      #print(paste("pid:", Sys.getpid()))
      if(purge_curl) {
        assignInNamespace('handle_pool', new.env(hash = TRUE, parent = emptyenv()), 'httr')
      }
      res <- f(...)
      return(res)
    }
    results <- mclapply(l, f, mc.cores=ncores, mc.cleanup=TRUE, mc.preschedule=preschedule)
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