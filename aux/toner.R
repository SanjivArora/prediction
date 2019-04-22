source("common.R")

require(dplyr)
require(memoise)
require(mlr)
require(itertools)
require(forcats)
require(magrittr)
require(plotly)
require(PTXQC)
require(tibble)


#sample_rate <- 1 #0.2
sample_rate <- 1
#max_models <- 3

#historical_jam_predictors = FALSE

selected_features <- FALSE


# For generating portable data sets, breaks modeling
#replace_nas=FALSE

#device_models <- device_groups[["trial_prod"]]
device_models <- device_groups[["trial_commercial"]]
#device_models <- device_groups[["e_series_commercial"]]

device_models <- c("E15")
#device_models <- c("G75")
#device_models <- c("V24")

# For toner analysis:
data_days <- 365
sc_code_days <- 0
sc_data_buffer <- 0
randomize_predictor_order <- FALSE

################################################################################
# Feature Names
################################################################################

if(selected_features) {
  fs <- readFeatures(feature_files)
} else {
  fs <- FALSE
}

################################################################################
# Sample dataset
################################################################################

file_sets <- getEligibleFileSets(regions, device_models, sources, sc_code_days, days=data_days, end_date=end_date)
data_files <- unlist(file_sets)

predictors_all <- dataFilesToDataset(
  data_files,
  sources,
  sample_rate,
  sc_code_days,
  delta_days=delta_days,
  deltas=deltas,
  only_deltas=only_deltas,
  features=fs,
  parallel=parallel
)

###############################################################################
# Clean and condition dataset
################################################################################

predictors_orig <- cleanPredictors(predictors_all, randomize_order=randomize_predictor_order, relative_replacement_dates = FALSE) %>% filterSingleValued
predictors <- predictors_orig

# Stats for dataset
print(paste(nrow(predictors), "total observations"))

################################################################################
# Restrict to valid numeric values
################################################################################

#predictors_eligible <- filterIneligibleFields(predictors, string_factors=factor_fields, exclude_cols=exclude_fields, replace_na=replace_nas)

################################################################################
# Toner exploration and graphing
################################################################################

#plotLatency(predictors_all)

# Plot distribution of predictor values for different model groups
# Get predictors with NAs
#ps_plot <- filterIneligibleFields(predictors, string_factors=factor_fields, exclude_cols=exclude_fields, replace_na=FALSE)
#n<-1
#n<-n+1; plotPredictorsForModels(ps_plot, fs[(1+n*9):(1+(n+1)*9)], c("E15", "E16"), c("E17", "E18"))
#n<-n+1; plotPredictorsForModels(ps_plot, fs[(1+n*9):(1+(n+1)*9)], c("E15"), c("E16"))
#n<-n+1; plotPredictorsForModels(ps_plot, row.names(top_features)[(1+n*9):(1+(n+1)*9)], c("E15", "E16"), c("G69", "G70"))


# Write processed data to s3
# s3saveRDS(predictors, 'data/predictors.RDS', 'ricoh-prediction-misc')
# s3saveRDS(service_codes, 'data/service_codes.RDS', 'ricoh-prediction-misc')
# s3saveRDS(responses, 'data/responses.RDS', 'ricoh-prediction-misc')

field <- "PMCount.X7931_11_Toner.Bottle.Bk.Serial.No.SP7.931.011"
grep('.*Toner.Bottle*', predictors %>% names()) -> pos
grep('.*Coverage.*', predictors %>% names()) -> pos
grep('(.*Toner.*)|GetDate.*|Serial', predictors %>% names()) -> pos
#predictors[predictors$Serial==serials[[4]],pos] %>% View

grep('.*Bottle*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*End.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Remain.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*PGS.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('Accumulation.*Coverage.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Current.Toner*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Remaining.Toner*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Toner.Bottle*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Waste.Toner.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Page.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Coverage.*K.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Developer*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Dev.Unit.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Vtref.Display.Setting.Current.Value.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*TD.Sens.Vt.Disp.Current.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Hum*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*PTR.Unit.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Drive.Distance.Counter.*Developer', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Replace.*PCU.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print
grep('.*Fusing.*', predictors %>% names()) -> p1; names(predictors)[p1] %>% print

#predictors[,p1] %>% View
serials <- predictors$Serial
serial <- serials[[1]]

colors <- c("K", "Y", "M", "C")
color_scheme=c("black", "gold1", "magenta2", "royalblue1")

coverage <- c(
  "Count.X8921_1_Pixel.Coverage.Accumulation.Coverage.BK.SP8.921.001",
  "Count.X8921_2_Pixel.Coverage.Accumulation.Coverage.Y.SP8.921.002", 
  "Count.X8921_3_Pixel.Coverage.Accumulation.Coverage.M.SP8.921.003", 
  "Count.X8921_4_Pixel.Coverage.Accumulation.Coverage.C.SP8.921.004" 
)

coverage_current <- c(
  "Count.X8921_1_Pixel.Coverage.Accumulation.Coverage.BK.SP8.921.001",
  "Count.X8921_2_Pixel.Coverage.Accumulation.Coverage.Y.SP8.921.002", 
  "Count.X8921_3_Pixel.Coverage.Accumulation.Coverage.M.SP8.921.003", 
  "Count.X8921_4_Pixel.Coverage.Accumulation.Coverage.C.SP8.921.004" 
)

toner <- c(
  "PMCount.X7931_12_Toner.Bottle.K.Remaining.Toner.SP7.931.012",
  "PMCount.X7934_12_Toner.Bottle.Y.Remaining.Toner.SP7.934.012",
  "PMCount.X7932_12_Toner.Bottle.M.Remaining.Toner.SP7.932.012",
  "PMCount.X7933_12_Toner.Bottle.C.Remaining.Toner.SP7.933.012"
)

pages_current <- c(
  "Count.X8891_1_Pages.Current.Toner.BK.SP8.891.001",
  "Count.X8891_2_Pages.Current.Toner.Y.SP8.891.002",
  "Count.X8891_3_Pages.Current.Toner.M.SP8.891.003",
  "Count.X8891_4_Pages.Current.Toner.C.SP8.891.004" 
)

pages_prev <- c(
  "Count.X8901_1_Pages.Current.Toner.BK.previous.SP8.901.001",
  "Count.X8901_2_Pages.Current.Toner.Y.previous.SP8.901.002",
  "Count.X8901_3_Pages.Current.Toner.M.previous.SP8.901.003",
  "Count.X8901_4_Pages.Current.Toner.C.previous.SP8.901.004"
)

developer_rotation <- c(
  "PMCount.X7942_4_PM.Counter.Rotation.Developer.K.SP7.942.004.Read.Only", 
  "PMCount.X7942_73_PM.Counter.Rotation.Developer.Y.SP7.942.073.Read.Only",
  "PMCount.X7942_50_PM.Counter.Rotation.Developer.M.SP7.942.050.Read.Only",              
  "PMCount.X7942_27_PM.Counter.Rotation.Developer.C.SP7.942.027.Read.Only"              
)

developer_usage_rate <- c(
  "PMCount.X7960_4_Estimated.Usage.Rate.Developer.K.SP7.960.004.Read.Only",    
  "PMCount.X7960_73_Estimated.Usage.Rate.Developer.Y.SP7.960.073.Read.Only",
  "PMCount.X7960_50_Estimated.Usage.Rate.Developer.M.SP7.960.050.Read.Only",             
  "PMCount.X7960_27_Estimated.Usage.Rate.Developer.C.SP7.960.027.Read.Only"
)

developer_pages <- c(
  "PMCount.X7954_4_PM.Counter.Page.Developer.K.SP7.954.004.Read.Only",
  "PMCount.X7954_73_PM.Counter.Page.Developer.Y.SP7.954.073.Read.Only",
  "PMCount.X7954_50_PM.Counter.Page.Developer.M.SP7.954.050.Read.Only",                  
  "PMCount.X7954_27_PM.Counter.Page.Developer.C.SP7.954.027.Read.Only"                  
)

developer_remaining <- c(
  "PMCount.X7956_4_Estimated.Remain.Days.Developer.K.SP7.956.004.Read.Only",
  "PMCount.X7956_73_Estimated.Remain.Days.Developer.Y.SP7.956.073.Read.Only",
  "PMCount.X7956_50_Estimated.Remain.Days.Developer.M.SP7.956.050.Read.Only",            
  "PMCount.X7956_27_Estimated.Remain.Days.Developer.C.SP7.956.027.Read.Only"            
)

developer_replacement_count <- c(
  "PMCount.X7853_4_Replacement.Cnt.Developer.K.SP7.853.004.Read.Only",
  "PMCount.X7853_73_Replacement.Cnt.Developer.Y.SP7.853.073.Read.Only",
  "PMCount.X7853_50_Replacement.Cnt.Developer.M.SP7.853.050.Read.Only",                  
  "PMCount.X7853_27_Replacement.Cnt.Developer.C.SP7.853.027.Read.Only"                  
)

developer_unit_remaining <- c(
  "PMCount.X7956_3_Estimated.Remain.Days.Dev.Unit.K.SP7.956.003.Read.Only",
  "PMCount.X7956_72_Estimated.Remain.Days.Dev.Unit.Y.SP7.956.072.Read.Only",
  "PMCount.X7956_49_Estimated.Remain.Days.Dev.Unit.M.SP7.956.049.Read.Only",         
  "PMCount.X7956_26_Estimated.Remain.Days.Dev.Unit.C.SP7.956.026.Read.Only"         
)

vtref <- c(
  "PMCount.X3230_1_Vtref.Display.Setting.Current.Value.K.SP3.230.001.Read.Only",
  "PMCount.X3230_4_Vtref.Display.Setting.Current.Value.Y.SP3.230.004.Read.Only",
  "PMCount.X3230_3_Vtref.Display.Setting.Current.Value.M.SP3.230.003.Read.Only",
  "PMCount.X3230_2_Vtref.Display.Setting.Current.Value.C.SP3.230.002.Read.Only"
)

td_sens <- c(
  "PMCount.X3210_1_TD.Sens.Vt.Disp.Current.K.SP3.210.001.Read.Only",
  "PMCount.X3210_4_TD.Sens.Vt.Disp.Current.Y.SP3.210.004.Read.Only",
  "PMCount.X3210_3_TD.Sens.Vt.Disp.Current.M.SP3.210.003.Read.Only",
  "PMCount.X3210_2_TD.Sens.Vt.Disp.Current.C.SP3.210.002.Read.Only"
)

developer_replacement_date <- c(
  "PMCount.X7950_3_Unit.Replacement.Date.Dev.Unit.K.SP7.950.003.Read.Only",
  "PMCount.X7950_72_Unit.Replacement.Date.Dev.Unit.Y.SP7.950.072.Read.Only",
  "PMCount.X7950_49_Unit.Replacement.Date.Dev.Unit.M.SP7.950.049.Read.Only",
  "PMCount.X7950_26_Unit.Replacement.Date.Dev.Unit.C.SP7.950.026.Read.Only"
)

pcu_distance <- c(
  "PMCount.X7944_2_PM.Counter.Display.Distance.PCU.K.SP7.944.002.Read.Only",
  "PMCount.X7944_71_PM.Counter.Display.Distance.PCU.Y.SP7.944.071.Read.Only",
  "PMCount.X7944_48_PM.Counter.Display.Distance.PCU.M.SP7.944.048.Read.Only",
  "PMCount.X7944_25_PM.Counter.Display.Distance.PCU.C.SP7.944.025.Read.Only"                 
)

pcu_replacement_date <- c(
"PMCount.X7950_2_Unit.Replacement.Date.PCU.K.SP7.950.002.Read.Only",
"PMCount.X7950_71_Unit.Replacement.Date.PCU.Y.SP7.950.071.Read.Only",
"PMCount.X7950_48_Unit.Replacement.Date.PCU.M.SP7.950.048.Read.Only",
"PMCount.X7950_25_Unit.Replacement.Date.PCU.C.SP7.950.025.Read.Only" 
)

waste_toner_replacement_date <- "PMCount.X7950_142_Unit.Replacement.Date.Waste.Toner.Bottle.SP7.950.142.Read.Only"

pages_total <- "Count.X8381_1_Total.Total.PrtPGS.SP8.381.001"


# Production
isProduction <- function(x) startswith(x, 'C')

if(device_models[[1]] %>% isProduction) {
  pages_total <- "Count.X8381_1_Total.Total.PrtPGS.SP8.381"
  
  toner <- c(
    "PMCount.X7931_12_Toner.Bottle.K.Remaining.Toner.SP7.931.012",
    "PMCount.X7934_12_Toner.Bottle.Y.Remaining.Toner.SP7.934.012",
    "PMCount.X7933_12_Toner.Bottle.M.Remaining.Toner.SP7.933.012",
    "PMCount.X7932_12_Toner.Bottle.C.Remaining.Toner.SP7.932.012"
  )
  
  # Different mechanism, appears to be a similar function
  developer_rotation <- c(
    "PMCount.X7942_4_Drive.Distance.Counter.K_Developer.SP7.942.004",
    "PMCount.X7942_73_Drive.Distance.Counter.Y_Developer.SP7.942.073",
    "PMCount.X7942_50_Drive.Distance.Counter.M_Developer.SP7.942.050",
    "PMCount.X7942_27_Drive.Distance.Counter.C_Developer.SP7.942.027"
  )
}
  
all_names <- concat(c(coverage, coverage_current, toner, pages_current, pages_prev, c(pages_total), developer_rotation))
missing <- setdiff(all_names, names(predictors))
if(length(missing) > 0) {
  print("Missing names:")
  print(missing)
}

deltas <- concat(c(coverage, toner, pages_current, pages_prev))
deltas %<>% append(c(pages_total))
deltas %<>% unlist

coverage_delta <- paste(coverage, 'delta', sep='.')
toner_delta <- paste(toner, 'delta', sep='.')
pages_current_delta <- paste(pages_current, 'delta', sep='.')
pages_prev_delta <- paste(pages_prev, 'delta', sep='.')
pages_delta <- paste(pages_total, 'delta', sep='.')

# Generated values
toner_replaced <- c(
  "Toner.Replaced.K",
  "Toner.Replaced.Y",
  "Toner.Replaced.M",
  "Toner.Replaced.C"
)

coverage_bottle <- c(
  "Toner.Bottle.Coverage.K",
  "Toner.Bottle.Coverage.Y",
  "Toner.Bottle.Coverage.M",
  "Toner.Bottle.Coverage.C"
)

coverage_prev_bottle <- c(
  "Toner.Bottle.Coverage.Previous.K",
  "Toner.Bottle.Coverage.Previous.Y",
  "Toner.Bottle.Coverage.Previous.M",
  "Toner.Bottle.Coverage.Previous.C"
)

min_observed_toner_prev_bottle <- c(
  "Toner.Min.Observed.Previous.K",
  "Toner.Min.Observed.Previous.Y",
  "Toner.Min.Observed.Previous.M",
  "Toner.Min.Observed.Previous.C"
)

# Toner consumption / pixel coverage
toner_per_coverage <- c(
  "Toner.Per.Coverage.K",
  "Toner.Per.Coverage.Y",
  "Toner.Per.Coverage.M",
  "Toner.Per.Coverage.C"
)

makePlotForSerial <- function(serial, traces, yaxis='y1', mode="lines+markers", type="scatter", plot=NA, dataset=NA) {
  if(identical(plot, NA)) {
    plot <- plot_ly()
  }
  if(identical(dataset, NA)) dataset <- predictors
  data <- dataset[dataset$Serial==serial,]
  # Prod
  #xdata <- data$Count.X8381_1_Total.Total.PrtPGS.SP8.381
  # Commercial
  #xdata <- data$Count.X8381_1_Total.Total.PrtPGS.SP8.381.001
  xdata <- data$GetDate
  for(t in traces) {
    plot %<>% add_trace(x=xdata, y=data[,t], type=type, name=t, mode=mode, yaxis=yaxis)#, yaxis=yaxis)
  }
  return(plot)
}

tonerForSerial <- function(serial, color=1, dataset=NA) {
  if(identical(dataset, NA)) dataset <- predictors
  p <- plot_ly(width=1400, height=1000)
  p %<>% layout(
    title=paste("Toner:", serial),
    #legend = list(x = 100, y = 50),
    #yaxis=list(title="Pages"),
    #yaxis2=list(title="Toner%", overlaying="y", side="right", range=c(0,100)),
    yaxis=list(rangemod='nonnegative'),
    yaxis2=list(overlaying="y", side="right", rangemode='nonnegative'),
    yaxis3=list(overlaying="y", side="right", rangemode='nonnegative'),
    yaxis4=list(overlaying="y", side="right", rangemode='nonnegative'),
    yaxis5=list(overlaying="y", side="right", rangemode='nonnegative')
  )
  #p <- makePlotForSerial(serial, toner[[color]], yaxis="y2", plot=p, dataset=dataset)
  #p <- makePlotForSerial(serial, coverage_current[[color]], yaxis="y3", plot=p, dataset=dataset)
  #p <- makePlotForSerial(serial, toner_per_coverage[[color]], yaxis="y4", plot=p, dataset=dataset)
  #p <- makePlotForSerial(serial, toner_replaced[[color]], yaxis="y5", plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, coverage[[color]], yaxis="y", plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, toner_per_coverage[[color]], yaxis="y2", plot=p, dataset=dataset)
  #p <- makePlotForSerial(serial, coverage_prev_bottle[[i]], plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, pages_prev[[color]], yaxis="y3", plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, developer_replacement_date[[color]], yaxis="y4", plot=p, dataset=dataset)
  #p <- makePlotForSerial(serial, "PMCount.X7956_142_Estimated.Remain.Days.Waste.Toner.Bottle.SP7.956.142.Read.Only", yaxis="y4", plot=p, dataset=dataset)
  #p <- makePlotForSerial(serial, "PMCount.X7950_142_Unit.Replacement.Date.Waste.Toner.Bottle.SP7.950.142.Read.Only", yaxis="y4", plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, developer_rotation[[color]], yaxis="y4", plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, toner[[color]], yaxis="y5", plot=p, dataset=dataset)
  #p <- makePlotForSerial(serial, coverage_prev_bottle[[color]], yaxis="y3", plot=p, dataset=dataset)
  print(p)
}

tonerForSerialMinimal <- function(serial, color=1, dataset=NA) {
  if(identical(dataset, NA)) dataset <- predictors
  p <- plot_ly(width=1400, height=1000)
  p %<>% layout(
    title=paste("Toner:", serial),
    yaxis=list(rangemod='nonnegative'),
    yaxis2=list(overlaying="y", side="right", rangemode='nonnegative'),
    yaxis3=list(overlaying="y", side="right", rangemode='nonnegative'),
    yaxis4=list(overlaying="y", side="right", rangemode='nonnegative'),
    yaxis5=list(overlaying="y", side="right", rangemode='nonnegative')
  )
  p <- makePlotForSerial(serial, coverage[[color]], yaxis="y", plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, pages_prev[[color]], yaxis="y2", plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, toner[[color]], yaxis="y3", plot=p, dataset=dataset)
  print(p)
}

replacementDates <- function(serial, color=1, dataset=NA) {
  if(identical(dataset, NA)) dataset <- predictors
  p <- plot_ly(width=1400, height=1000)
  p %<>% layout(
    title=paste("Toner:", serial),
    yaxis=list(rangemod='nonnegative'),
    yaxis2=list(overlaying="y", side="right", rangemode='nonnegative'),
    yaxis3=list(overlaying="y", side="right", rangemode='nonnegative'),
    yaxis4=list(overlaying="y", side="right", rangemode='nonnegative'),
    yaxis5=list(overlaying="y", side="right", rangemode='nonnegative'),
    yaxis6=list(overlaying="y", side="right", rangemode='nonnegative'),
    yaxis7=list(overlaying="y", side="right", rangemode='nonnegative')
    
  )
  p <- makePlotForSerial(serial, toner[[color]], yaxis="y", plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, coverage[[color]], yaxis="y2", plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, developer_replacement_date[[color]], yaxis="y3", plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, waste_toner_replacement_date, yaxis="y4", plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, pcu_replacement_date[[color]], yaxis="y5", plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, developer_rotation[[color]], yaxis="y6", plot=p, dataset=dataset)
  p <- makePlotForSerial(serial, pcu_distance[[color]], yaxis="y6", plot=p, dataset=dataset)
  
  print(p)
}

LCSn <- function(seqs) {
  Reduce(LCS, seqs)
}

tonerScatterHistForSerials <- function(
    serials,
    traces=list(),
    data=NA,
    create_labels=FALSE,
    colors=color_scheme,
    datasets=NA,
    mode='markers',
    log_x=FALSE,
    log_y=FALSE,
    force_cmyk=FALSE,
    opacity=0.7
  ) {
  if(identical(datasets, NA)) {
    datasets <- list(predictors[predictors$Serial %in% serials,])
  }
  xlabels <- list()
  ylabels <- list()
  xhist <- plot_ly()
  yhist <- plot_ly()
  p <- plot_ly(width=1400, height=1000)
  for(i in 1:length(traces)) {
    t <- traces[[i]]
    print(t)
    trace_name <- t[[3]]
    x_all <- list()
    y_all <- list()
    for(df in datasets) {
      df %<>% filter(Serial %in% serials)
      xy <- df[,t[1:2]]
      df <- df[complete.cases(xy),]
      xy <- xy[complete.cases(xy),]
      sers <- df$Serial %>% unique
      if (length(sers)==0 ) {
        next
      } else if(length(sers) > 1) {
        name <- trace_name
      } else {
        name <- paste(trace_name, sers)
      }
      x <- xy[,1]
      y <- xy[,2]
      x_all %<>% append(x)
      y_all %<>% append(y)
      if(log_x) x %<>% log
      if(log_y) y %<>% log
      print(x)
      print(y)
      if(length(t) > 3) hover_fields <- t[[4]] else hover_fields <- c('Serial')
      if(create_labels) {
        hover_labels <- lapply(1:nrow(df), function(j) {
          paste(hover_fields, df[j, hover_fields], collapse='\n', sep=": ")
        })
      } else {
        hover_labels <- ""
      }
      # Use arbitrary colors if we have multiple datasets, otherwise use CMYK by trace
      if(length(datasets) != 1 && !force_cmyk) {
        p %<>% add_trace(x=x, y=y, type='scatter', name=name, mode=mode, text=~hover_labels, opacity=opacity)
      } else {
        color=I(colors[[i]])
        p %<>% add_trace(x=x, y=y, type='scatter', name=name, mode=mode, color=color, text=~hover_labels, opacity=opacity)
      }
    }
    xlabels %<>% append(t[[1]])
    ylabels %<>% append(t[[2]])
    xhist %<>% add_trace(x=concat(x_all), type='histogram', name=paste("x", "hist", trace_name), color=I(colors[[i]]))
    yhist %<>% add_trace(y=concat(y_all), type='histogram', name=paste("y", "hist", trace_name), color=I(colors[[i]]))
  }
  # Axis labels are the longest common subsequence set of names used
  xlabel <- LCSn(xlabels)
  ylabel <- LCSn(ylabels)
  p %<>% layout(xaxis=list(title=xlabel), yaxis=list(title=ylabel))
  all <- subplot(
    xhist, 
    plotly_empty(), 
    p, 
    yhist,
    nrows = 2, heights = c(0.2, 0.8), widths = c(0.8, 0.2), 
    shareX = TRUE, shareY = TRUE, titleX = TRUE, titleY = TRUE
  )
  print(all)
}

# KYMC histogram
plotHist <- function(xs, names=NA, cumulative=F, color=color_scheme, histnorm='') {
  if(identical(names, NA)) names <- colors
  p <- plot_ly(width=1400, height=1000)
  for(i in 1:length(xs)) {
    p %<>% add_trace(x=xs[[i]], type='histogram', name=names[[i]], color=I(color[[i]]), histnorm=histnorm, cumulative=list(enabled=cumulative))
  }
  print(p)
}

# KYMC histogram
plotDensity <- function(xs, names=NA, cumulative=T, color=color_scheme) {
  if(identical(names, NA)) names <- colors
  p <- plot_ly(width=1400, height=1000)
  for(i in 1:length(xs)) {
    fit <- density(xs[[i]] %>% na.omit)
    p %<>% add_trace(x = fit$x, y = fit$y, mode = "lines", name=names[[i]], color=I(color[[i]]))
  }
  print(p)
}

tonerScatterForSerials <- function(serials, traces=list()) {
  p <- plot_ly(width=1400, height=1000)
  data <- predictors[predictors$Serial %in% serials,]
  xlabels <- list()
  ylabels <- list()
  for(t in traces) {
    print(t)
    x <- data[,t[[1]]]
    y <- data[,t[[2]]]
    xlabels %<>% append(t[[1]])
    ylabels %<>% append(t[[2]])
    name <- t[[3]]
    # Axis labels are the longest common subsequence set of names used
    p %<>% add_trace(x=x, y=y, type='scatter', name=name, mode='markers')
  }
  xlabel <- LCSn(xlabels)
  ylabel <- LCSn(ylabels)
  p %<>% layout(xaxis=list(title=xlabel), yaxis=list(title=ylabel))
  print(p)
}

add_delta <- function(df, source_name) {
  name <- paste(source_name, 'delta', sep='.')
  data <- df[,source_name]
  df[,name] <- c(NA, tail(data, length(data) - 1) - head(data, length(data) - 1))
  return(df)
}

# Check if toner cartridge has been replace by comparing pages printed for current and previous bottles with prior reading
add_toner_replacement <- function(df) {
  for(i in 1:length(colors)) {
    cur <- df[,pages_current_delta[[i]]]
    prev <- df[,pages_prev_delta[[i]]]
    df[,toner_replaced[[i]]] <- cur<0 | prev!=0
  }
  return(df)
}

estimate_bottle_coverage <- function(df) {
  for(i in 1:length(colors)) {
    df[,coverage_prev_bottle[[i]]] <- NA
    if(nrow(df) < 2) next
    bottle_start <- NA
    prev_bottle_start <- NA
    min_observed_prev <- NA
    # Index for earliest readings of current bottle when we are tracking efficiency
    toner_efficiency_start_index <- 1
    df[, toner_per_coverage[[i]]] <- NA
    for(j in 2:nrow(df)) {
      if(df[j, toner_replaced[[i]]] %>% isTRUE) {
        # Estimate total coverage at the point when the bottle was changed by allocating coverage since last reading by toner bottle page count
        prev_bottle_start <- bottle_start
        pages_delta <- df[j, pages_current[[i]]] + df[j, pages_prev[[i]]] - df[j-1, pages_current[[i]]]
        bottle_start <- df[j-1,coverage[[i]]] + ((pages_delta - df[j,pages_current[[i]]]) / pages_delta) * df[j,coverage_delta[[i]]]
        
        # Toner was just replaced, so the lowest observed coverage for the last bottle was the previous reading
        min_observed_prev <- df[j-1,toner[[i]]]
        
        bottle_first_reading_index <- i
        # If pixel coverage has jumped dramatically we can't make a good estimate (e.g. possiblity of multiple replacement) so reset values to NA
        if(df[j,coverage_delta[[i]]] > 3*1e4) {
          prev_bottle_start <- NA
          bottle_start <- NA
          min_observed_prev <- NA
        }
        # Ditto for first replacement as the initial toner bottle may be a reduced-capacity SKU
        if(!df[j-1, pages_prev[[i]]]) {
          prev_bottle_start <- NA
          bottle_start <- NA
          min_observed_prev <- NA
        }
      }
      
      if(!identical(NA, prev_bottle_start)) {
        bottle_coverage <- bottle_start - prev_bottle_start
        df[j, coverage_prev_bottle[[i]]] <- bottle_coverage
      }
      if(!identical(NA, bottle_start)) {
        current_coverage <- df[j, coverage[[i]]] - bottle_start
        df[j, coverage_bottle[[i]]] <- current_coverage
      }
      if(!identical(NA, min_observed_prev)) {
         df[j, min_observed_toner_prev_bottle[[i]]] <- min_observed_prev
      }
      
      # Production machines have an internal toner hopper and report ~20-25% for an empty bottle
      if(df[j,]$Serial %>% isProduction) {
        stop_tracking_at <- 30
      } else {
        stop_tracking_at <- 5
      }
      # Calculate toner efficiency where we have a clean toner and coverage deltas for a given bottle + developer unit combination
      # Check both replacement dates and developer rotation/yield as some machines don't necessarily record replacement date for new units (e.g. E15)
      developer_replaced <- !identical(df[j, developer_replacement_date[[i]]], df[j-1, developer_replacement_date[[i]]]) || (df[j, developer_rotation[[i]]] < df[j-1, developer_rotation[[i]]])
      # This has the minor drawback of dropping the last sample
      data_end <- j == nrow(df)
      if(toner_efficiency_start_index && (isTRUE(df[j, toner_replaced[[i]]]) || isTRUE(df[j, toner[[i]]] <= stop_tracking_at) || developer_replaced || data_end)) {
        # print(df[j,"Serial"])
        # print(j)
        # print(toner_efficiency_start_index)
        # print(df[j, toner_replaced[[i]]])
        # print(isTRUE(df[j, toner[[i]]] == 0))
        # print("")
        
        tracked_delta_toner <- (df[toner_efficiency_start_index, toner[[i]]] - df[j-1, toner[[i]]])
        tracked_delta_coverage <- (df[j-1, coverage[[i]]] - df[toner_efficiency_start_index, coverage[[i]]])
        
        # Only calcualte a value when error due to granularity of toner level measurement is reasonable
        if(is.numeric(tracked_delta_toner) && isTRUE(tracked_delta_toner > 20)) {
          df[j-1, toner_per_coverage[[i]]] <-  tracked_delta_toner / tracked_delta_coverage
        }

        toner_efficiency_start_index <- FALSE
      }
      # Only resume tracking when we have a toner replacement followed by a reading without a toner replacement
      if(df[j-1, toner_replaced[[i]]] %>% isTRUE && df[j, toner_replaced[[i]]] %>% isFALSE) {
        toner_efficiency_start_index <- j-1
      }
    }
  }
  return(df)
}

process_serial_df <- function(df) {
  df <- df[order(df$RetrievedDateTime),]
  for(name in deltas) {
    df %<>% add_delta(name)
  }
  df %<>% add_toner_replacement
  df %<>% estimate_bottle_coverage
  return(df)
}

serial_dfs <- split(predictors_orig, predictors_orig$Serial)
serial_dfs <- plapply(serial_dfs, process_serial_df, parallel=T)
predictors <- bindRowsForgiving(serial_dfs)

# Add location data
locations <- withCloudFile('s3://ricoh-prediction-misc/locations.csv', read.csv)
locations$Serial <- locations$SerialNo
predictors %<>% join(locations, by='Serial')

# Fine readings for current developer unit

# Examine efficiency
get_eff <- function(preds) {
  eff <- preds[, toner_per_coverage]
  hits <- apply(eff, 1, function(r) !(r %>% is.na %>% all))
  result <- preds[hits,]
  return(result)
}
pred_eff <- predictors %>% get_eff

# Find toner per coverage values for current developer units
# TODO: E15 seems to have a single rotation value for color dev units and doesn't appear to automatically update the replacement date
pred_current <- predictors
for(i in 1:length(colors)) {
  field <- developer_replacement_date[[i]]
  p <- pred_current
  p %<>% rownames_to_column('rowname')
  p %<>% group_by(Serial)
  p %<>% filter(!!sym(field) == max(!!sym(field)))
  pred_current[,toner_per_coverage[[i]]] <- NA
  pred_current[p$rowname, toner_per_coverage[[i]]] <- p[,toner_per_coverage[[i]]]
}
pred_eff_current <- pred_current %>% get_eff
eff_current <- pred_eff_current[, toner_per_coverage]

traces <- list()
for(i in 1:length(colors)) {
  traces %<>% append(list(
    #c(coverage_delta[[i]], toner_delta[[i]], colors[[i]])
    #c(pages_prev[[i]], coverage_prev_bottle[[i]], colors[[i]])
    #c(min_observed_toner_prev_bottle[[i]], coverage_prev_bottle[[i]], colors[[i]])
    #c(toner_per_coverage[[i]], "PMCount.X7956_142_Estimated.Remain.Days.Waste.Toner.Bottle.SP7.956.142.Read.Only", colors[[i]])
    #c(toner_per_coverage[[i]], pages_total, colors[[i]])
    #c(toner_per_coverage[[i]], "Serial", colors[[i]])
    #c(toner_per_coverage[[i]], "RetrievedDateTime", colors[[i]])
    #c(toner_per_coverage[[i]], developer_rotation[[i]], colors[[i]])
    c(developer_rotation[[i]], toner_per_coverage[[i]], colors[[i]])
    #c(toner_per_coverage[[i]], developer_pages[[i]], colors[[i]])
    #c(toner_per_coverage[[i]], developer_remaining[[i]], colors[[i]])
    #c(toner_per_coverage[[i]], developer_replacement_count[[i]], colors[[i]])
    #c(toner_per_coverage[[i]], developer_unit_remaining[[i]], colors[[i]])
    #c(toner_per_coverage[[i]], vtref[[i]], colors[[i]])
    #c(toner_per_coverage[[i]], td_sens[[i]], colors[[i]])
    #c(toner_per_coverage[[i]], "PMCount.X7953_16_Operation.Env.Log.PCU.Bk.25.Temp.30.80.Hum.100.SP7.953.016.Read.Only", colors[[i]])
    #c(toner_per_coverage[[i]], "PMCount.X7953_8_Operation.Env.Log.PCU.Bk.5.Temp.15.80.Hum.100.SP7.953.008.Read.Only", colors[[i]])
    #c(toner_per_coverage[[i]], developer_replacement_date[[i]], colors[[i]])
    #c(toner_per_coverage[[i]], "PMCount.X7942_109_PM.Counter.Display.Distance.PTR.Unit.SP7.942.109.Read.Only", colors[[i]])
    #c(toner_per_coverage[[i]], "PMCount.X7956_109_Estimated.Remain.Days.PTR.Unit.SP7.956.109.Read.Only", colors[[i]])
    #c(toner_per_coverage[[i]], "PMCount.X7960_109_Estimated.Usage.Rate.PTR.Unit.SP7.960.109.Read.Only", colors[[i]])
    #c(toner_per_coverage[[i]], "PMCount.X7621_109_PM.Counter.Display.Pages.PTR.Unit.SP7.621.109.Read.Only", colors[[i]])
    #c(toner_per_coverage[[i]], pcu_distance[[i]], colors[[i]])
    #c("GetDate", developer_rotation[[i]], colors[[i]])
    )
  )
}

pred_eff_by_serial <- split(pred_eff, pred_eff$Serial)

#tonerScatterHistForSerials(pred_eff$Serial, traces, datasets=list(pred_eff))
tonerScatterHistForSerials(pred_eff$Serial, traces, datasets=pred_eff_by_serial, mode='lines', force_cmyk = T)
tonerScatterHistForSerials(pred_eff$Serial, list(traces[[2]]), datasets=pred_eff_by_serial, mode='lines', log_y = F)

tonerScatterHistForSerials(unique(pred_eff$Serial) %>% sample %>% head(2), list(traces[[2]]), datasets=pred_eff_by_serial, mode='lines')


pred_eff_current_by_serial <- split(pred_eff_current, pred_eff_current$Serial)
tonerScatterHistForSerials(pred_eff_current$Serial, traces, datasets=pred_eff_current_by_serial, mode='lines')



print(paste("Summary of toner per coverage for:", paste(device_models)))
# Summary ignores digits argument, must set option
options(digits=10)
summary(eff_current)


# Find efficiencies for current toner unit in machines with one or more outliers, on a per-color basis
# TODO: look for correlation with customer and industry type

eff_current_median <- apply(eff_current, 2, . %>% na.omit %>% median)
eff_current_cutoff <- eff_current_median * 3
candidates <- pred_eff_current
for(i in 1:length(colors)) {
  # Find values that meet the threshold
  idx <- (candidates[,toner_per_coverage[[i]]] >= eff_current_cutoff[[i]]) %>% which
  idx_neg <- (candidates[,toner_per_coverage[[i]]] < eff_current_cutoff[[i]]) %>% which
  sers <- candidates[idx,]$Serial %>% unique
  sers_neg <- setdiff(candidates$Serial, sers)
  candidates[candidates$Serial %in% sers_neg, toner_per_coverage[[i]]] <- NA
}
# Filter out rows with no toner_per_coverage values
candidates_idxs <- apply(candidates[,toner_per_coverage], 1, function(r) !(r %>% is.na %>% all))
candidates <- candidates[candidates_idxs,] 

plotHist(candidates[,toner_per_coverage], cumulative=F)
plotDensity(candidates[,toner_per_coverage])

candidates_by_serial <- split(candidates, candidates$Serial)
tonerScatterHistForSerials(candidates$Serial, traces, datasets=candidates_by_serial, mode='line', log_y=F, force_cmyk = T)


for(i in 1:length(colors)) {
  cat(paste("Outlying serials for", colors[[i]]))
  cat("\n")
  idxs <- candidates[,toner_per_coverage[[i]]] %>% is.na %>% not
  sers <- candidates[idxs,]$Serial %>% unique %>% sort
  writeLines(sers)
  cat("\n")
}

by_s <- pred_eff %>% group_by(Serial)
by_s[,append(c('Serial'), toner_per_coverage)] %>% dplyr::summarise_all(funs(mean(., na.rm=T), sd(., na.rm=T)))


#tonerForSerial(serials[[5]])
#tonerForSerial("E163M450041")
#tonerForSerial("E163M750100")
#tonerForSerial("E163M850072", 2)
#tonerForSerial("E163MA50192", 2)

#tonerForSerialMinimal("G756R840065")
# 
# tonerForSerial("E175M950201", 2)
# tonerForSerial("E175M950223", 2)
# tonerForSerial("E175M950227", 2)
# tonerForSerial("E175M950047", 2)
# tonerForSerial("E175M950072", 2)
# tonerForSerial("E175M950335", 2)
# tonerForSerial("E175M950078", 2)
# 
# tonerForSerial("E173M950189", 4)
# tonerForSerial("E175M950047", 2)
# tonerForSerial("E175MA50349", 2)
# 
# tonerForSerial('C087C450017', 2)


# # Predict high toner use per coverage
# ps <- predictors[!is.na(predictors$Toner.Per.Coverage.Y),]
# ps_eligible <- filterIneligibleFields(ps, string_factors=factor_fields, exclude_cols=exclude_fields, replace_na=replace_nas)
# 
# used_labels <- c("Toner.Usage.High.Y", "Toner.Usage.High.M")
# responses <- ps_eligible$Toner.Per.Coverage.Y %>% as.data.frame
# responses[,1] <- ps_eligible$Toner.Per.Coverage.Y > 0.002
# responses[,2]<- ps_eligible$Toner.Per.Coverage.M > 0.002
# names(responses) <- used_labels
# # Train models in parallel as despite native threading support there are substantial serial sections
# models <- trainModelSet(used_labels, ps_eligible, responses, ntree=ntree, parallel=F)
# 
# for(label in keys(models)) {
#   cat("\n\n")
#   print(paste("Importance for", label))
#   showModelFeatureImportance(models[[label]], n=30)
# }



i <- 2
fs <- c(
  toner_per_coverage[[i]],
  developer_rotation[[i]],
  developer_replacement_date[[i]],
  "RegnState",
  "PMCount.X7950_71_Unit.Replacement.Date.PCU.Y.SP7.950.071.Read.Only",
  "PMCount.X7950_115_Unit.Replacement.Date.Fusing.Unit.SP7.950.115.Read.Only",                  
  "PMCount.X7950_116_Unit.Replacement.Date.Fusing.Belt.SP7.950.116.Read.Only"
)
plot_ly(
  type='parcoords',
  dimensions = lapply(fs[1:4], function(f) list(label=f, values=pred_eff[,f] %>% as.numeric)),
  line = list(color=(pred_eff[,fs[[1]]] %>% log))
)

cov_for_period <- group_by(predictors, Serial) %>% dplyr::summarize(cov_period_y = sum(!!sym(coverage_delta[[2]]), na.rm=T), replacements_y=sum(!!sym(toner_replaced[[2]]), na.rm=T), eff_y=mean(!!sym(toner_per_coverage[[2]]), na.rm=T))
cov_for_period <- cov_for_period[order(-cov_for_period$cov_period_y),]
cov_for_period <- cov_for_period[order(-cov_for_period$replacements_y),]


sers <- c(
# Normal
'E174M350014',
'E176M340565',
'E174M850154',
'E175M950042',
'E175M950023',
# Problem
'E175M950201',
'E175M950227',
'E175M950047',
'E175M950335'
# Incipient
#'E175M950128'
)
for(ser in sers) {
  e <- pred_eff[pred_eff$Serial==ser,toner_per_coverage]
  print(ser)
  print(e)
  print(cov_for_period[cov_for_period$Serial==ser,])
}

for(ser in sers) {
  tonerForSerial(ser,2)
}


by_regn <- group_by(predictors, RegnState) %>% dplyr::summarize(machines=length(unique(Serial)), pages = sum(!!sym(pages_delta), na.rm=T), cov.y = sum(!!sym(coverage_delta[[2]]), na.rm=T), replacements.y=sum(!!sym(toner_replaced[[2]]), na.rm=T), toner.per.y=mean(!!sym(toner_per_coverage[[2]]), na.rm=T))
by_regn %>% View
plot_ly(x=by_regn$RegnState, y=by_regn$toner.per.y)                                                                

effs <- eff_current[eff_current[,toner_per_coverage] < 0.01, toner_per_coverage]
plotHist(effs)

tonerForSerial("E154MA50090",2)
replacementDates("E154MA50090",2)

i = 1
by_ser <- group_by(predictors[predictors$Serial %in% candidates$Serial,], Serial) %>% dplyr::summarize(machines=length(unique(Serial)), pages = sum(!!sym(pages_delta), na.rm=T), cov.color = sum(!!sym(coverage_delta[[i]]), na.rm=T), replacements.color=sum(!!sym(toner_replaced[[i]]), na.rm=T), toner.per.color=mean(!!sym(toner_per_coverage[[i]]), na.rm=T))
by_ser %>% View
