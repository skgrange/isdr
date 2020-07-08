#' Function to download ISD data files.
#' 
#' @param file Vector of remote file names. 
#' 
#' @param directory_output Directory to export files to.
#' 
#' @param parallel Should files be read in parallel? If \code{TRUE}, a 
#' \strong{futures} back-end must be registered. 
#' 
#' @param verbose Should the function give messages? 
#' 
#' @author Stuart K. Grange. 
#' 
#' @return Invisible NULL. 
#' 
#' @seealso \href{https://www.ncdc.noaa.gov/isd}{ISD}
#' 
#' @examples 
#' 
#' \dontrun{
#' 
#' # Download some files
#' isd_download_data(
#'   file = "https://www.ncei.noaa.gov/data/global-hourly/access/2020/11290099999.csv", 
#'   directory_output = "~/Desktop"
#' )
#' 
#' }
#' 
#' @export
isd_download_data <- function(file, directory_output, parallel = FALSE, 
                              verbose = FALSE) {
  
  # Download all files
  if (parallel) {
    
    file %>% 
      furrr::future_walk(
        ~isd_download_data_worker(
          ., 
          directory_output = directory_output,
          verbose = FALSE
        ),
        .progress = verbose
      )
    
  } else {
    
    file %>% 
      purrr::walk(
        ~isd_download_data_worker(
          ., 
          directory_output = directory_output,
          verbose = verbose
        )
      )
    
  }
  
  return(invisible(NULL))
  
}


isd_download_data_worker <- function(file, directory_output, verbose) {
  
  # Read data
  df <- isd_read_worker(file, priority = FALSE, longer = FALSE, verbose = verbose)
  
  # Only export if something is there to be exported
  if (nrow(df) != 0) {
    
    # Build file name
    file_split <- stringr::str_split_fixed(file, "/|\\.", 12)
    
    file_output <- stringr::str_c(
      "isd_data_", file_split[, 11], "_", file_split[, 10], ".rds"
    )
    
    # Add directory
    file_output <- fs::path(directory_output, file_output)
    
    # Export as rds object
    saveRDS(df, file_output)
    
  } else{
    if (verbose) message(threadr::date_message(), "No observations available...")
  }
  
  return(invisible(NULL))
  
}