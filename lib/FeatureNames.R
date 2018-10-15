

get_feature_names <- function(fs, all_names) {
  
  # Check for mismatched feature names
  missing <- setdiff(fs, all_names)
  
  if(!is_empty(missing)) {
    print("Warning, the following features are specified but not found:")
    lapply(unlist(missing), print)
  }
  found_fs <- unlist(intersect(all_names, fs))
  if (is_empty(found_fs)) {
    print("No valid features specified, using all possible features")
    found_fs <- all_names
  }
  found_fs <- unlist(found_fs)
  return(found_fs)
}