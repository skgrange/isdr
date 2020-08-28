#' Function to squash R check's global variable notes. 
#' 
if (getRversion() >= "2.15.1") {
  
  # What variables are causing issues?
  variables <- c(
    ".", "station", "site", "site_name", "wd", "ws", "air_temp", "rh", 
    "atmospheric_pressure", "variable", "wnd", "wd", "ws", "x", "y", "z", "vis",
    "visibility", "name", "dew", "dew_point", "flag_atmospheric_pressure", 
    "flag_dew", "flag_temp", "slp", "tmp", "country", "country_iso_code",
    "current", "elevation", "latitude", "longitude", "site_name_smonitor", 
    "site_old", "site_smonitor", "aa1", "precipitation_code", "precipitation_code_1",
    "precipitation_code_2", "precipitation", "date_end", "value",
    "precipitation_12", "precipitation_6", "precipitation_6_lag", 
    "precipitation_corrected", "precipitation_corrected_pushed"
  )
  
  # Squash the note
  utils::globalVariables(variables)
  
}

