# Common imports for programs

require(rstudioapi)

# debugSource is an RStudio facility to allow in-fine source examination when debugging.
# This isn't available if running on the command line, so check whether we are in the RStudio environment.
if(!rstudioapi::isAvailable()) {
  debugSource <- source
}

debugSource("lib/Parallel.R")
debugSource("lib/Util.R")
debugSource("lib/Dates.R")
debugSource("lib/MemoiseCache.R")
debugSource("lib/Visualization.R")
debugSource("lib/Feature.R")
debugSource("lib/FeatureSelection.R")
debugSource("lib/Feature.R")
debugSource("lib/SCFile.R")
debugSource("lib/Sampling.R")
debugSource("lib/Logging.R")
debugSource("lib/DataFile.R")
debugSource("lib/Model.R")
debugSource("lib/Results.R")
debugSource("lib/Evaluate.R")
debugSource("lib/Cleaning.R")
debugSource("lib/SC.R")
debugSource("lib/Code.R")
debugSource("lib/Augment.R")
debugSource("lib/Response.R")
debugSource("lib/Split.R")
debugSource("lib/JamFile.R")
debugSource("defaults.R")