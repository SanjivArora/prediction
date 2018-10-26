ncores = detectCores() / 2

# Wrap parallel lapply implementation to allow easy debugging and change of backend
plapply <- function(l, f) {
  mclapply(l, f, mc.cores=ncores, mc.cleanup=TRUE)
}