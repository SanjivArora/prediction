require(logging)

log_level="DEBUG"

basicConfig(log_level)

getModuleLogger <- function(name) {
  logger <- getLogger(name)
  logger$setLevel("DEBUG")
  return(logger)
}