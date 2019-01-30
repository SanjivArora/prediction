require(Xmisc)


getDeviceModels <- function(...) {
  res <- device_groups[[device_group]]
  return(res)
}

makeParser <- function() {
  parser <- ArgumentParser$new()
  parser$add_argument(
    '--device_group', type='character',
    help='Label for device model set to use',
    default=default_device_group
  )
  parser$add_argument(
    '--email_to', type='character',
    help='Email address for prediction results',
    default=default_email_to
  )
  parser$add_argument(
    '--no_email', type='logical',
    action='store_true',
    help='Email prediction results to the specified address'
  )
  
  return(parser)
}
