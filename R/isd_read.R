#' Function to read ISD data files. 
#' 
#' @param file Vector of file names. 
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
#' isd_read(
#'   "https://www.ncei.noaa.gov/data/global-hourly/access/2020/11290099999.csv"
#' )
#' 
#' isd_read(
#'   "https://www.ncei.noaa.gov/data/global-hourly/access/2020/11290099999.csv",
#'   priority = TRUE
#' )
#' 
#' @export
isd_read <- function(file, priority = FALSE, longer = FALSE, parallel = FALSE,
                     verbose = FALSE) {
  
  if (parallel) {
    
    df <- file %>%
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
    
    df <- file %>%
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


isd_read_worker <- function(file, priority, longer, verbose) {
  
  # Parse tabular data
  df <- read_isd_table(file, verbose = verbose)
  
  if (priority) {
    df <- df %>% 
      separate_and_clean_isd_variables() %>% 
      select(site = station,
             site_name,
             date,
             wd,
             ws,
             air_temp, 
             rh,
             atmospheric_pressure) %>% 
      openair::timeAverage(avg.time = "hour", type = c("site", "site_name")) %>% 
      ungroup() %>% 
      mutate(across(c(site, site_name), as.character))
  }
  
  # Reshape data
  if (longer) {
    df <- df %>% 
      tidyr::pivot_longer(-c(site, site_name, date), names_to = "variable") %>% 
      arrange(variable,
              date)
  }
  
  return(df)
  
}


separate_and_clean_isd_variables <- function(df) {
  
  # Clean wind variables
  df <- df %>% 
    tidyr::separate(
      wnd, into = c("wd", "x", "y", "ws", "z"), sep = ",", convert = TRUE
    ) %>% 
    mutate(wd = if_else(wd == 999, NA_integer_, wd),
           ws = if_else(ws == 9999, NA_integer_, ws),
           ws = ws / 10) %>% 
    select(-x,
           -y,
           -z)
  
  # Clean visibility variables
  df <- df %>% 
    tidyr::separate(
      vis, 
      into = c("visibility", "flag_vis1", "flag_vis2", "flag_vis3"), 
      sep = ",", 
      convert = TRUE
    ) %>% 
    mutate(
      visibility = if_else(visibility %in% c(9999, 999999), NA_integer_, visibility)
    ) %>% 
    select(-dplyr::matches("flag_vis"))
  
  # Clean air temperature variables
  df <- df %>% 
    tidyr::separate(
      tmp, into = c("air_temp", "flag_temp"), sep = ",", convert = TRUE
    ) %>% 
    mutate(air_temp = if_else(air_temp == 9999, NA_integer_, air_temp),
           air_temp = air_temp / 10) %>% 
    select(-flag_temp)
  
  # Clean dew point variables
  df <- df %>% 
    tidyr::separate(
      dew, into = c("dew_point", "flag_dew"), sep = ",", convert = TRUE
    ) %>% 
    mutate(dew_point = if_else(dew_point == 9999, NA_integer_, dew_point),
           dew_point = dew_point / 10) %>% 
    select(-flag_dew)
  
  # Clean pressure variables
  df <- df %>% 
    tidyr::separate(
      slp, 
      into = c("atmospheric_pressure", "flag_atmospheric_pressure"), 
      sep = ",", 
      convert = TRUE
    ) %>% 
    mutate(
      atmospheric_pressure = if_else(
        atmospheric_pressure %in% c(9999, 99999), NA_integer_, atmospheric_pressure
      ),
      atmospheric_pressure = atmospheric_pressure / 10
    ) %>% 
    select(-flag_atmospheric_pressure)
  
  # # Clean precipitation variables
  # df %>% 
  #   tidyr::separate(
  #     aa1, 
  #     into = c(
  #       "precipitation_code", "precipitation", "precipitation_code_1", 
  #       "precipitation_code_2"
  #     ), 
  #     sep = ",", 
  #     convert = TRUE
  #   )
  
  # ceiling height? 
  
  # Radiation variables?
  
  # Calculate relative humidity
  df <- mutate(
    df,
    rh = 100 * ((112 - 0.1 * air_temp + dew_point)/(112 + 0.9 * air_temp)) ^ 8
  )
  
  # Clean site name
  df <- df %>% 
    mutate(site_name = stringr::str_split_fixed(name, ",", 2)[, 1]) %>% 
    select(-name)
  
  return(df)
  
}


read_isd_table <- function(file, verbose) {
  
  # Message to user
  if (verbose) message(threadr::date_message(), "`", file, "`...")
  
  if (stringr::str_detect(file, ".csv$")) {
    
    if (stringr::str_detect(file, "^http")) {
      
      # Remote, reassign file object
      file <- isd_get_remote_text(file)
      
      # Return if file does not exist
      if (length(file) == 0) return(tibble())
      
    } 
    
    # Parse content, warning suppression is for non-declared names
    df <- suppressWarnings(
      readr::read_csv(
        file, 
        col_types = isd_variable_types(), 
        guess_max = 100000,
        progress = FALSE
      ) 
    ) %>% 
      dplyr::rename_all(threadr::str_to_underscore) %>%
      mutate(date = lubridate::force_tz(date, tzone = "UTC"))
    
  } else if (stringr::str_detect(file, ".rds$")) {
    df <- readRDS(file)
  }
  
  return(df)
  
}


isd_variable_types <- function() {
  
  readr::cols_only(
    STATION = readr::col_character(),
    DATE = readr::col_datetime(format = ""),
    SOURCE = readr::col_double(),
    LATITUDE = readr::col_double(),
    LONGITUDE = readr::col_double(),
    ELEVATION = readr::col_double(),
    NAME = readr::col_character(),
    REPORT_TYPE = readr::col_character(),
    CALL_SIGN = readr::col_double(),
    QUALITY_CONTROL = readr::col_character(),
    WND = readr::col_character(),
    CIG = readr::col_character(),
    VIS = readr::col_character(),
    TMP = readr::col_character(),
    DEW = readr::col_character(),
    SLP = readr::col_character(),
    AA1 = readr::col_character(),
    AW1 = readr::col_character(),
    GA1 = readr::col_character(),
    GA2 = readr::col_character(),
    GA3 = readr::col_character()
  )
  
}


isd_get_remote_text <- function(file) {
  
  # Use httr to get page
  response <- httr::GET(file)
  
  # If file does not exist
  if (httr::status_code(response) == 404L) return(as.character())
  
  # Extract content as text
  text <- httr::content(response, type = "text", encoding = "UTF-8")
  
  return(text)
  
}
