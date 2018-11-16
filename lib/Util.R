require(hash)
require(plyr)
require(rlang)

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

# Take a list/vector of objects and a function mapping objects to keys, return a hash of objects grouped by string representation of key
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
sortBy <- function(xs, f, desc=FALSE) {
  xs <- valuesIfHash(xs)
  if(length(xs) == 0) {
    return(xs)
  }
  res <- xs[order(unlist(lapply(xs, f)))]
  if(desc) {
    res <- rev(res)
  }
  return(res)
}

# Count empty objects as false
isTrue <- function(x) {
  !is_empty(x) && isTRUE(x)
}

# Filter list/vector/hash argument to values that are true where f(value) is true according to isTrue
filterBy <- function(xs, f) {
  if (is.hash(xs)) {
    filterX <- filterHashByValue
  } else {
    filterX <- filterListBy
  }
  res <- filterX(xs, f)
  return(res)
}

# filterBy implementation for lists/vectors
filterListBy <- function(xs, f) {
  hits <- list()
  for(x in xs) {
    if(isTrue(f(x))) {
      hits <- append(hits, list(x))
    }
  }
  return(hits)
}

# filterBy implementation for hashes
filterHashByValue <- function(h, f) {
  res <- hash()
  for(k in keys(h)) {
    val <- h[[k]]
    if(isTrue(f(val))) {
      res[[k]] <- val
    }
  }
  return(res)
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
splitDataFrame <- function(df) {
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

# Combine hashes, values in the second hash override values in the first
updateHash <- function(h1, h2) {
  hash(append(keys(h1), keys(h2)), append(values(h1), values(h2)))
}

# Combine hashes, values in the second hash are added to values in the first hash
addHash <- function(h1, h2) {
  res <- copy(h1)
  for(k in keys(h2)) {
    res[[k]] <- getWithDefault(h1, k, 0) + h2[[k]]
  }
  return(res)
}

# Return first hash without keys from second hash
subtractHash <- function(h1, h2) {
   res <- hash()
   for(k in keys(h1)) {
    if (!has.key(k, h2)[[1]]) {
      res[[k]] <- h1[[k]]
    }
   }
   return(res)
}


# Convert value to integer, maintaining average value. I.e. taking the mean of repeated applications of this function to x will converge to x.
toIntegerStochastic <- function(x) {
  integral_portion <- floor(x)
  if(runif(1) < x - integral_portion) {
    return(integral_portion + 1)
  } else {
    return(integral_portion)
  }
}

# Deterministically select entire dataset if length(x) <= size, and repeate until length(x) > size.
# At this point sample randomly without replacement, allowing non-integral size such that the average size is correct over repeated applications.
stochasticSelection <- function(x, size) {
  selected <- list()
  while(size >= length(x)) {
    selected <- append(selected, x)
    size <- size - length(x)
  }
  sampled <- sample(x, toIntegerStochastic(size), replace=FALSE)
  selected <- append(selected, sampled)
  return(selected)
}

testStochasticSelection <- function(target_frac=1.2) {
  l <- 1:3
  l1 <- list()
  n <- 10000
  for(x in 1:n) {
    l1 <- append(l1, stochasticSelection(l, target_frac))
  }
  print(paste("Approximate target: ", n*target_frac, ", Actual: ", length(l1)))
}

peek <- function(df, x=5, y=5) {
  df[1:x, 1:y]
}

bindRowsForgiving <- function(dfs) {
  # TODO: better solution for variable data structure
  rbind.fill(dfs)
}

# Take values from xs such that each value of f(x) is unique. Order of selection is undefined, and f(x) must be able to be cast to a string.
uniqueBy <- function(xs, f) {
  h <- hash()
  for(x in xs) {
    h[[as.character(f(x))]] <- x
  }
  return(values(h, simplify=FALSE))
}

require(data.table)
rbind.fill.DT <- function(ll) {
  # changed sapply to lapply to return a list always
  all.names <- lapply(ll, names)
  unq.names <- unique(unlist(all.names))
  ll.m <- rbindlist(lapply(seq_along(ll), function(x) {
    tt <- ll[[x]]
    setattr(tt, 'class', c('data.table', 'data.frame'))
    data.table:::settruelength(tt, 0L)
    invisible(alloc.col(tt))
    tt[, c(unq.names[!unq.names %chin% all.names[[x]]]) := NA_character_]
    setcolorder(tt, unq.names)
  }))
}
