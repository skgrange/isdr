#' Function to get metadata for the NOAA's Integrated Surface Database (ISD) 
#' sites. 
#' 
#' @author Stuart K. Grange
#' 
#' @param file File name of ISD sites data. If empty, the remote version is 
#' used.
#' 
#' @param clean Should the returned tibble be cleaned in the \strong{smonitor} 
#' style? 
#' 
#' @return Tibble. 
#' 
#' @seealso \href{https://www.ncdc.noaa.gov/isd}{ISD}
#' 
#' @examples 
#' 
#' # Get isd sites data
#' get_isd_sites()
#' 
#' @export
get_isd_sites <- function(file = NA, clean = TRUE) {
  
  # Load data
  if (is.na(file)) {
    file <- "ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv"
  }
  
  # Keep data types robust
  col_types <- readr::cols(
    USAF = readr::col_character(),
    WBAN = readr::col_double(),
    `STATION NAME` = readr::col_character(),
    CTRY = readr::col_character(),
    STATE = readr::col_character(),
    ICAO = readr::col_character(),
    LAT = readr::col_double(),
    LON = readr::col_double(),
    `ELEV(M)` = readr::col_double(),
    BEGIN = readr::col_double(),
    END = readr::col_double()
  )
  
  # Read
  df <- readr::read_csv(file, progress = FALSE, col_types = col_types)
  
  if (clean) {
    
    # Clean names
    names(df) <- threadr::str_to_underscore(names(df))
    names(df) <- if_else(names(df) == "station_name", "site_name", names(df))
    names(df) <- if_else(names(df) == "ctry", "country", names(df))
    names(df) <- if_else(names(df) == "lat", "latitude", names(df))
    names(df) <- if_else(names(df) == "lon", "longitude", names(df))
    names(df) <- if_else(names(df) == "elev(m)", "elevation", names(df))
    names(df) <- if_else(names(df) == "begin", "date_start", names(df))
    names(df) <- if_else(names(df) == "end", "date_end", names(df))
    
    # Build site variables
    df$site <- stringr::str_c(df$usaf, df$wban)
    df$site_old <- stringr::str_c(df$usaf, df$wban, sep = "-")
    df[, c("usaf", "wban")] <- NULL
    
    # Site name
    df$site_name <- if_else(df$site_name == "...", NA_character_, df$site_name)
    df$site_name <- stringr::str_squish(df$site_name)
    
    # Catch missingness
    df$elevation <- if_else(df$elevation %in% c(-999.9, -999), NA_real_, df$elevation)
    df$latitude <- if_else(df$latitude == 0, NA_real_, df$latitude)
    df$longitude <- if_else(df$longitude == 0, NA_real_, df$longitude)
    
    # Parse dates
    df$date_start <- lubridate::ymd(df$date_start, tz = "UTC")
    df$date_end <- lubridate::ymd(df$date_end, tz = "UTC")
    
    # Calculate period
    df$period <- threadr::to_period(df$date_start, df$date_end, as.character = TRUE)
    
    # Is the site current? 
    df$current <- if_else(
      df$date_end >= lubridate::now(tzone = "UTC") - lubridate::days(30), 
      TRUE, 
      FALSE
    )
    
    # Join country iso codes too
    df <- left_join(df, isd_country_codes(), by = "country")
    
    # Missing countries
    df$country_iso_code <- if_else(is.na(df$country_iso_code), "XX", df$country_iso_code)
    
    # Add smonitor site code too
    df$site_smonitor <- create_smonitor_site_code(df$site_old, df)$site_smonitor
    
    # Arrange the variables
    df <- df %>% 
      select(current, 
             site,
             site_old,
             site_name, 
             site_smonitor,
             latitude, 
             longitude, 
             elevation, 
             country,
             country_iso_code,
             everything()) 
    
  }
  
  return(df)
  
}


#' Function to convert an ISD site code to a \strong{smonitor} site code.
#' 
#' @param site Vector of sites to clean. 
#' 
#' @param df Tibble from \code{get_isd_sites} if already retrieved. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Tibble. 
#' 
#' @export
create_smonitor_site_code <- function(site, df = NA) {
  
  # Get site table if needed
  if (!any(grepl("data.frame", class(df))) && is.na(df[1])) {
    df <- get_isd_sites(clean = TRUE)
  } 
  
  # Format
  df <- df %>% 
    mutate(site_smonitor = isd_site_to_smonitor_site(site_old, country_iso_code),
           site_name_smonitor = stringr::str_to_title(site_name))
  
  # Filter to input and select
  df <- df %>% 
    filter(site_old %in% !!site) %>% 
    select(site, 
           site_name,
           site_smonitor,
           site_name_smonitor,
           country)
  
  return(df)
  
}


#' Function clean an ISD site code to a \strong{smonitor} site code.
#' 
#' @param site_isd A vector of ISD site codes. 
#' 
#' @param country Country suffix. This will generally be the ISO code. 
#'
#' @author Stuart K. Grange
#'
#' @return Character vector.
#'
#' @export
isd_site_to_smonitor_site <- function(site_isd, country) {
  
  x <- stringr::str_remove_all(site_isd, "-99999$|-999999$|^99999-|^999999-") 
  x <- if_else(grepl("-", x), stringr::str_split_fixed(x, "-", 2)[, 1], x)
  x <- stringr::str_c(stringr::str_to_lower(country), x, "i")
  return(x)
  
}


isd_country_codes <- function() {

  tibble::tribble(                                                                                                                                                                                    
    ~country, ~country_iso_code,
    "AA",     "AW",             
    "AC",     "AG",             
    "AE",     "AE",             
    "AF",     "AF",             
    "AG",     "DZ",             
    "AJ",     "AZ",             
    "AL",     "AL",             
    "AM",     "AM",             
    "AO",     "AO",             
    "AQ",     "AS",             
    "AR",     "AR",             
    "AS",     "AU",             
    "AU",     "AT",             
    "AV",     "AI",             
    "AY",     "AQ",             
    "AZ",     "PT",             
    "BA",     "QA",             
    "BB",     "BB",             
    "BC",     "BW",             
    "BD",     "BM",             
    "BE",     "BE",             
    "BF",     "BS",             
    "BG",     "BD",             
    "BH",     "BZ",             
    "BK",     "BA",             
    "BL",     "BO",             
    "BM",     "MM",             
    "BN",     "BJ",             
    "BO",     "BY",             
    "BP",     "SB",             
    "BR",     "BR",             
    "BT",     "BT",             
    "BU",     "BG",             
    "BV",     "NO",             
    "BX",     "BN",             
    "BY",     "RW",             
    "CA",     "CA",             
    "CB",     "KH",             
    "CD",     "TD",             
    "CE",     "LK",             
    "CF",     "CG",             
    "CG",     "CD",             
    "CH",     "CN",             
    "CI",     "CL",             
    "CJ",     "KY",             
    "CK",     "CC",             
    "CM",     "CM",             
    "CN",     "KM",             
    "CO",     "CO",             
    "CP",     "ES",             
    "CQ",     "MP",             
    "CS",     "CR",             
    "CT",     "CF",             
    "CU",     "CU",             
    "CV",     "CV",             
    "CW",     "CW",             
    "CY",     "CY",             
    "DA",     "DK",             
    "DJ",     "DJ",             
    "DO",     "DO",             
    "DQ",     "DQ",             
    "DR",     "DR",             
    "EC",     "EC",             
    "EG",     "EG",             
    "EI",     "IE",             
    "EK",     "EK",             
    "EN",     "EE",             
    "ER",     "ER",             
    "ES",     "ES",             
    "ET",     "ET",             
    "EU",     "EU",             
    "EZ",     "CZ",             
    "FG",     "FG",             
    "FI",     "FI",             
    "FJ",     "FJ",             
    "FK",     "FK",             
    "FM",     "FM",             
    "FO",     "FO",             
    "FP",     "FP",             
    "FQ",     "FQ",             
    "FR",     "FR",             
    "FS",     "FR",             
    "GA",     "GA",             
    "GB",     "GB",             
    "GG",     "GE",             
    "GH",     "GH",             
    "GI",     "GI",             
    "GJ",     "GJ",             
    "GK",     "GG",             
    "GL",     "GL",             
    "GM",     "DE",             
    "GP",     "GP",             
    "GQ",     "GU",             
    "GR",     "GR",             
    "GT",     "GT",             
    "GV",     "GV",             
    "GY",     "GY",             
    "HA",     "HA",             
    "HK",     "HK",             
    "HO",     "HO",             
    "HR",     "HR",             
    "HU",     "HU",             
    "IC",     "IS",             
    "ID",     "ID",             
    "IM",     "IM",             
    "IN",     "IN",             
    "IO",     "IO",             
    "IR",     "IR",             
    "IS",     "IL",             
    "IT",     "IT",             
    "IV",     "IV",             
    "IZ",     "IQ",             
    "JA",     "JA",             
    "JE",     "JE",             
    "JM",     "JM",             
    "JO",     "JO",             
    "JQ",     "JQ",             
    "JU",     "JU",             
    "KE",     "KE",             
    "KG",     "KG",             
    "KN",     "KN",             
    "KR",     "KR",             
    "KS",     "KS",             
    "KT",     "KT",             
    "KU",     "KU",             
    "KV",     "XK",             
    "KZ",     "KZ",             
    "LA",     "LA",             
    "LE",     "LE",             
    "LG",     "LV",             
    "LH",     "LT",             
    "LI",     "LI",             
    "LO",     "SK",             
    "LQ",     "LQ",             
    "LS",     "LI",             
    "LT",     "LT",             
    "LU",     "LU",             
    "LY",     "LY",             
    "MA",     "MA",             
    "MB",     "MB",             
    "MC",     "MC",             
    "MD",     "MD",             
    "MF",     "MF",             
    "MG",     "MG",             
    "MH",     "MH",             
    "MI",     "MI",             
    "MJ",     "ME",             
    "MK",     "MK",             
    "ML",     "ML",             
    "MO",     "MA",             
    "MP",     "MP",             
    "MQ",     "MQ",             
    "MR",     "MR",             
    "MT",     "MT",             
    "MU",     "MU",             
    "MV",     "MV",             
    "MX",     "MX",             
    "MY",     "MY",             
    "MZ",     "MZ",             
    "NC",     "NC",             
    "NE",     "NE",             
    "NF",     "NF",             
    "NG",     "NG",             
    "NH",     "NH",             
    "NI",     "NI",             
    "NL",     "NL",             
    "NN",     "NN",             
    "NO",     "NO",             
    "NP",     "NP",             
    "NR",     "NR",             
    "NS",     "NS",             
    "NT",     "NT",             
    "NU",     "NU",             
    "NZ",     "NZ",             
    "OD",     "OD",             
    "PA",     "PA",             
    "PC",     "PC",             
    "PE",     "PE",             
    "PK",     "PK",             
    "PL",     "PL",             
    "PM",     "PM",             
    "PN",     "PN",             
    "PO",     "PT",             
    "PP",     "PP",             
    "PS",     "PS",             
    "PU",     "PU",             
    "QA",     "QA",             
    "RE",     "RE",             
    "RI",     "RS",             
    "RM",     "RM",             
    "RO",     "RO",             
    "RP",     "RP",             
    "RQ",     "RQ",             
    "RS",     "RU",             
    "RW",     "RW",             
    "SA",     "SA",             
    "SB",     "SB",             
    "SC",     "SC",             
    "SE",     "SE",             
    "SF",     "SF",             
    "SG",     "SG",             
    "SH",     "SH",             
    "SI",     "SI",             
    "SL",     "SL",             
    "SN",     "SN",             
    "SO",     "SO",             
    "SP",     "ES",             
    "ST",     "ST",             
    "SU",     "SU",             
    "SV",     "SV",             
    "SW",     "SE",             
    "SX",     "SX",             
    "SY",     "SY",             
    "SZ",     "CH",             
    "TD",     "TD",             
    "TE",     "TE",             
    "TH",     "TH",             
    "TI",     "TI",             
    "TK",     "TK",             
    "TL",     "TL",             
    "TN",     "TN",             
    "TO",     "TO",             
    "TP",     "TP",             
    "TS",     "TS",             
    "TT",     "TT",             
    "TU",     "TR",             
    "TV",     "TV",             
    "TW",     "TW",             
    "TX",     "TX",             
    "TZ",     "TZ",             
    "UC",     "UC",             
    "UG",     "UG",             
    "UK",     "GB",             
    "UP",     "UA",             
    "US",     "US",             
    "UV",     "UV",             
    "UY",     "UY",             
    "UZ",     "UZ",             
    "VC",     "VC",             
    "VE",     "VE",             
    "VI",     "VI",             
    "VM",     "VM",             
    "VQ",     "VQ",             
    "WA",     "WA",             
    "WF",     "WF",             
    "WI",     "WI",             
    "WQ",     "WQ",             
    "WS",     "WS",             
    "WZ",     "WZ",             
    "YM",     "YM",             
    "ZA",     "ZA",             
    "ZI",     "ZI"
  )
    
}
