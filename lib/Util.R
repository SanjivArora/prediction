require(hash)

substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}

# If object is a hash, return list of values, otherwise return original object
valuesIfHash <- function(x) {
  if(class(x)=="hash") {
    return(values(x, simplify=FALSE))
  } else {
    return(x)
  }
}

# Take a list/vector of objects and a function mapping objects to keys, return a list of objects grouped by string representation of key
groupBy <- function(xs, f) {
  xs <- valuesIfHash(xs)
  h <- hash()
  for(x in xs) {
    key <- f(x)
    # Convert key to string to work around lack of broad type support in the hash library
    key <- toString(key)
    if(has.key(key, h)[[1]]) {
      l <- h[[key]]
    } else {
      l <- list()
    }
    h[[key]] <- append(l, list(x))
  }
  return(h)
}

# Take a list/vector of objects and a function mapping object to orderable key, and return objects sorted by key
sortBy <- function(xs, f) {
  xs <- valuesIfHash(xs)
  if(length(xs) == 0) {
    return(xs)
  }
  res <- xs[order(unlist(lapply(xs, f)))]
  return(res)
}

# Takea list/vector of objects and a boolean function and return objects where function is true
filterBy <- function(xs, f) {
  xs <- valuesIfHash(xs)
  hits <- list()
  for(x in xs) {
    # Count empty objects as false
    val <- f(x)
    if(!is_empty(val) && f(x)) {
      hits <- append(hits, list(x))
    }
  }
  return(hits)
}

# Join paths, with absolute paths replacing previous components
joinPaths <- function(...) {
  joinTwo <- function(p1, p2) {
    if(isAbsolutePath(p2)) {p2} else {filePath(p1, p2)}
  }
  parts <- list(...)
  while(length(parts) > 1) {
    p1 <- joinTwo(parts[[1]], parts[[2]])
    parts <- append(list(p1), tail(parts, length(parts)-2))
  }
  return(parts[[1]])
}

# Return a list of individual row dataframes
splitDataframe <- function(df) {
  res <- list()
  len <- nrow(df)
  if(len==0) {return(res)}
  for(i in 1:len) {
    res <- append(res, list(df[i,]))
  }
  return(res)
}

# Attempt to get a value from a hash. If the key exists return the value, otherwise return the default value.
getWithDefault <- function(h, key, default) {
  if(has.key(key, h)[[1]]) {
    return(h[[key]])
  } else {
    return(default)
  }
}