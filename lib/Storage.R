isS3Path <- function(path) {
  startsWith(path, "s3://")
}

# Join paths while respecting "s3://" URIs
pathJoin <- function(p1, p2) {
  paste(p1, "/", p2, sep="")
}

# Accept a URI or path and bucket name, and a callback function.
# Execute the callback function with the path to a local temporary copy
# of the specified cloud file, valid for the duration of the call. 
# Return the result of the callback function.
withCloudFile <- function(uri_or_path, f, bucket=NA) {
  p <- tempfile()
  tryCatch(
    {
      if(is.na(bucket)) {
        save_object(uri_or_path, file=p)
      } else {
        save_object(uri_or_path, bucket, file=p)
      }
      res <- f(p)
    },
    # If there is an exception, rethrow
    error=function(e) {
      stop(e)
    },
    # Always delete the temporary file
    finally={
      unlink(p)
    }
  )
  return(res)
}

# Return path of last modified cloud file for specified bucket and prefix
latestCloudFile <- function(bucket, prefix="") {
  files <- get_bucket(bucket, prefix=prefix)
  if(length(files) == 0) {
    return(NA)
  }
  latest <- sortBy(files, function(f) f$LastModified, desc=TRUE)[[1]]
  return(latest$Key)
}
