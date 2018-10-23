require(hash)

substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}

# Take a list/vector of objects and a function mapping objects to keys, return a list of objects grouped by string representation of key
groupBy <- function(xs, f) {
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
    h[[key]] <- append(l, x)
  }
  return(h)
}

# Take a list/vector of objects and a function mapping object to orderable key, and return objects sorted by key
sortBy <- function(xs, f) {
  xs[order(unlist(lapply(xs, f)))]
}

# Takea list/vector of objects and a boolean function and return objects where function is true
filterBy <- function(xs, f) {
  hits <- list()
  for(x in xs)
    if(f(x)) {
      hits <- append(hits, x)
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
