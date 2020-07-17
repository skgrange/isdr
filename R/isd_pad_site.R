#' Function to pad ISD site code. 
#' 
#' @author Stuart K. Grange and Nelson the Duck. 
#' 
#' @param x Vector of ISD sites. 
#' 
#' @return Character vector. 
#' 
#' @examples 
#' 
#' isd_pad_site(03772099999)
#' 
#' @export
isd_pad_site <- function(x) stringr::str_pad(x, width = 11, pad = "0")
