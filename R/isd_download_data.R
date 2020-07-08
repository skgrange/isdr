#' Function to download ISD data files.
#' 
#' @param file_remote Vector of remote file names. 
#' 
#' @param file_local Vector of file names to export data to. 
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
isd_download_data <- function(file_remote, file_local, parallel = FALSE, 
                              verbose = FALSE) {
  
  # Download all files
  if (parallel) {
    
      furrr::future_walk2(
        file_remote, 
        file_local,
        ~isd_download_data_worker(
          file_remote = .x, 
          file_local = .y,
          verbose = FALSE
        ),
        .progress = verbose
      )
    
  } else {
    
    purrr::walk2(
      file_remote, 
      file_local,
      ~isd_download_data_worker(
        file_remote = .x, 
        file_local = .y,
        verbose = verbose
      )
    )
    
  }
  
  return(invisible(NULL))
  
}


isd_download_data_worker <- function(file_remote, file_local, verbose) {
  
  # Read data
  df <- isd_read_worker(file_remote, priority = FALSE, longer = FALSE, verbose = verbose)
  
  # Only export if something is there to be exported
  if (nrow(df) != 0) {
    
    # Export as an rds object
    if (fs::path_ext(file_local) == "rds") {
      saveRDS(df, file_local)
    } else {
      stop("Output format not supported.", call. = FALSE)
    }
    
  } else{
    if (verbose) message(threadr::date_message(), "No observations available...")
  }
  
  return(invisible(NULL))
  
}
