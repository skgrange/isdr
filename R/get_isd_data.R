#' Function to directly get/import ISD data.
#' 
#' @param site Vector of ISD site codes. 
#' 
#' @param year Vector of years.  
#' 
#' @param priority Should only a selected priority group of variables be 
#' returned? 
#' 
#' @param longer Should the observations be reshaped to be in "long" format? 
#' 
#' @param parallel Should files be read in parallel? If \code{TRUE}, a 
#' \strong{futures} back-end must be registered. 
#' 
#' @param verbose Should the function give messages? 
#' 
#' @author Stuart K. Grange. 
#' 
#' @return Tibble. 
#' 
#' @seealso \href{https://www.ncdc.noaa.gov/isd}{ISD}
#' 
#' @examples 
#' 
#' # Load an example data file
#' get_isd_data(site = "11290099999", year = 2020, priority = TRUE)
#' 
#' @export
get_isd_data <- function(site, year, priority = FALSE, longer = FALSE,
                         parallel = FALSE, verbose = FALSE) {
  
  # Build remote file names
  file_remote <- isd_build_urls(site, year)
  
  # Read all the files
  if (parallel) {
    
    df <- file_remote %>%
      furrr::future_map_dfr(
        ~isd_read_worker(
          .,
          priority = priority,
          longer = longer,
          verbose = FALSE
        ),
        .progress = verbose
      )
    
  } else {
    
    df <- file_remote %>%
      purrr::map_dfr(
        ~isd_read_worker(
          .,
          priority = priority,
          longer = longer,
          verbose = verbose
        )
      )
    
  }
  
  return(df)
  
}


#' Function to build ISD remote file names.
#' 
#' @param site Vector of ISD site codes. 
#' 
#' @param year Vector of years.  
#' 
#' @author Stuart K. Grange. 
#' 
#' @return Character vector.  
#' 
#' @examples 
#' 
#' isd_build_urls(c("08495099999", "03334099999", "08536099999"), 2000:2002)
#' 
#' @export
isd_build_urls <- function(site, year) {
  
  # Build table of site and year combinations
  df <- tidyr::crossing(
    site = unique(site),
    year = unique(year)
  )
  
  # Build remote urls
  x <- stringr::str_c(
    "https://www.ncei.noaa.gov/data/global-hourly/access/",
    df$year, "/",
    df$site,
    ".csv"
  )
  
  return(x)
  
}
