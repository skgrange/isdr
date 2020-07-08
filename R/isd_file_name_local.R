#' Function to build local file name from ISD data file URL. 
#' 
#' @param file Vector of remote ISD data file URLs. 
#' 
#' @param directory Directory to add to file name (optional). 
#' 
#' @param extension File type extension to append.  
#' 
#' @author Stuart K. Grange
#' 
#' @return \strong{fs} path. 
#' 
#' @examples 
#' 
#' isd_file_name_local(
#'   "https://www.ncei.noaa.gov/data/global-hourly/access/2018/11290099999.csv"
#' )
#' 
#' isd_file_name_local(
#'   "https://www.ncei.noaa.gov/data/global-hourly/access/2018/11290099999.csv",
#'   directory = "~/Desktop"
#' )
#' 
#' @export
isd_file_name_local <- function(file, directory = NA, extension = "rds") {
  
  # Split file name
  file_split <- stringr::str_split_fixed(file, "/|\\.", 12)
  
  # Build local file name
  x <- stringr::str_c(
    "isd_data_", file_split[, 11], "_", file_split[, 10]
  ) %>% 
    fs::path_ext_set(extension)
  
  # Add directory
  if (!is.na(directory[1])) x <- fs::path(directory, x)
  
  return(x)
  
}
