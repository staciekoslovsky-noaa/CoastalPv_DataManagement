# Coastal Pv Surveys: Process effort date/time and tidal covariates
# S. Hardy, 06MAR2018

# Create functions -----------------------------------------------
# Function to install packages needed
install_pkg <- function(x)
{
  if (!require(x,character.only = TRUE))
  {
    install.packages(x,dep=TRUE)
    if(!require(x,character.only = TRUE)) stop("Package not found")
  }
}

# Install libraries ----------------------------------------------
# Installation of exifr requires portable perl to be stored at the following location on computer: C:\Strawberry!!!
install_pkg("RPostgreSQL")

# Run code -------------------------------------------------------
# Set working directory of where xtide.exe is stored
setwd ("C:\\xtide")

# Functions for extracting tide data
get_tide_height <- function(tide_dt, station) {
  begin <- format(tide_dt, format = "%Y-%m-%d %H:%M:%S", tz = "GMT")
  end <- tide_dt + 60
  end <- format(end, format = "%Y-%m-%d %H:%M:%S", tz = "GMT")
  argument <- paste(' -z -u m -em pSsMm -f c -m m -l "', station, '" -b "', begin, '" -e "', end, '"', sep = "")
  tide_csv <- readr::read_csv(
    paste0( 
      system2("tide", argument, stdout = TRUE, stderr = FALSE), 
      "\n"),
    col_names = FALSE
  )
  if(nrow(tide_csv) > 0){
    height <- tide_csv[[1, 4]]
  } else {
    height <- -99
  }
}

get_nearest_hi_lo <- function(tide_dt, station, tide_type) {
  begin <- tide_dt - (3600*12)
  begin <- format(begin, format = "%Y-%m-%d %H:%M:%S",tz = "GMT")
  end <- tide_dt + (3600*12)
  end <- format(end, format = "%Y-%m-%d %H:%M:%S",tz = "GMT")
  
  if (tide_type == "High") {tide_state <- "High Tide"} else 
    if (tide_type == "Low") {tide_state <- "Low Tide"}
  
  tmp.csv <- tempfile()
  argument <- paste(' -z -u m -em pSsMm -f c -l "', station, '" -b "', begin, '" -e "', end, '"', sep = "")
  system2("tide", argument, stdout = tmp.csv, stderr = FALSE)
  tides <- readr::read_csv(tmp.csv, col_names = FALSE)
  unlink(tmp.csv)
  
  tides <- tides[tides$X5 == tide_state, ]
  tides$X6 <- paste(tides$X2, tides$X3)
  tides$X6 <- as.POSIXct(tides$X6, format = "%Y-%m-%d %I:%M %p", tz = "GMT")
  tide <- tides[which.min(abs(difftime(tide_dt, tides$X6, units = "secs"))), ]
  tide_list <- list(height = NA, time = NA)
  if(nrow(tide) > 0){
    tide_list <- list(height = as.numeric(strsplit(as.character(tide$X4), " ")[[1]][1]), time = tide$X6)
  }
  return(tide_list)
}

# Connect to DB
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_user"), 
                              password = Sys.getenv("user_pw"))

# Tide processing for ADFG data
# dbSendQuery(con, paste("UPDATE surv_pv_cst.archive_poly_counts_adfg SET tide_height = NULL",
#                        ", nearest_high_dt = NULL",
#                        ", nearest_high_height = NULL",
#                        ", nearest_low_dt = NULL",
#                        ", nearest_low_height = NULL", sep = ""))
adfg <- dbGetQuery(con, "SELECT polyid, survey_dt, station FROM surv_pv_cst.archive_poly_counts_adfg INNER JOIN surv_pv_cst.geo_polys USING (polyid) WHERE tide_height IS NULL")

if (nrow(adfg) > 0) {
  adfg$tide_dt <- as.POSIXct(adfg$survey_dt, tz = "America/Vancouver")
  attributes(adfg$tide_dt)$tzone <- "GMT"
  
  for (i in 1:nrow(adfg)){
    # Set processing variables
    station <- adfg$station[i]
    tide_dt <- adfg$tide_dt[i]
    polyid <- adfg$polyid[i]
    
    # Extract tidal data
    height <- get_tide_height(tide_dt, station)
    high <- get_nearest_hi_lo(tide_dt, station, "High")
    low <- get_nearest_hi_lo(tide_dt, station, "Low")
    
    high_height <- high[[1]][1]
    high_dt <- high[[2]][1]
    
    low_height <- low[[1]][1]
    low_dt <- low[[2]][1]
    
    # Update DB
    if (is.na(height) == FALSE & is.na(high_height) == FALSE & is.na(high_dt) == FALSE & is.na(low_height) == FALSE & is.na(low_dt) == FALSE) {
      dbSendQuery(con, paste("UPDATE surv_pv_cst.archive_poly_counts_adfg SET tide_height = ", height,
                             ", nearest_high_dt = \'", high_dt,
                             "\', nearest_high_height = ", high_height,
                             ", nearest_low_dt = \'", low_dt,
                             "\', nearest_low_height = ", low_height,
                             " WHERE polyid = \'", polyid,
                             "\' AND survey_dt = \'", tide_dt, 
                             "\'", sep = ""))
    } else {
      dbSendQuery(con, paste("UPDATE surv_pv_cst.archive_poly_counts_adfg SET tide_height = -99",
                             ", nearest_high_height = -99", 
                             ", nearest_low_height = -99", 
                             " WHERE polyid = \'", polyid,
                             "\' AND survey_dt = \'", tide_dt, 
                             "\'", sep = ""))
    }
    rm(station, tide_dt, polyid, height, high, high_dt, high_height, low, low_dt, low_height)
  }
  rm(i)
}

# Tide processing for NOAA data
# dbSendQuery(con,  paste("UPDATE surv_pv_cst.tbl_effort SET tide_height = NULL",
#                        ", nearest_high_dt = NULL",
#                        ", nearest_high_height = NULL",
#                        ", nearest_low_dt = NULL",
#                        ", nearest_low_height = NULL", sep = ""))
noaa <- dbGetQuery(con, "SELECT e.id, survey_dt, station 
                   FROM surv_pv_cst.tbl_effort e
                   INNER JOIN surv_pv_cst.geo_polys
                   USING (polyid) 
                   WHERE tide_height IS NULL")
if (nrow(noaa) > 0) {
  noaa$tide_dt <- as.POSIXct(noaa$survey_dt, tz = "America/Vancouver")
  attributes(noaa$tide_dt)$tzone <- "GMT"
  
  for (i in 1:nrow(noaa)){
    if (noaa$survey_dt[i] == '1111-01-01 01:11:11') {
      dbSendQuery(con, paste("UPDATE surv_pv_cst.tbl_effort SET tide_height = -99",
                             ", nearest_high_height = -99", 
                             ", nearest_low_height = -99", 
                             " WHERE id = ", id, sep = ""))
    } else {
      # Set processing variables
      station <- noaa$station[i]
      tide_dt <- noaa$tide_dt[i]
      id <- noaa$id[i]
      
      # Extract tidal data
      height <- get_tide_height(tide_dt, station)
      high <- get_nearest_hi_lo(tide_dt, station, "High")
      low <- get_nearest_hi_lo(tide_dt, station, "Low")
      
      high_height <- high[[1]][1]
      high_dt <- high[[2]][1]
      
      low_height <- low[[1]][1]
      low_dt <- low[[2]][1]
      
      # Update DB
      if (is.na(height) == FALSE & is.na(high_height) == FALSE & is.na(high_dt) == FALSE & is.na(low_height) == FALSE & is.na(low_dt) == FALSE) {
        dbSendQuery(con, paste("UPDATE surv_pv_cst.tbl_effort SET tide_height = ", height,
                               ", nearest_high_dt = \'", high_dt,
                               "\', nearest_high_height = ", high_height,
                               ", nearest_low_dt = \'", low_dt,
                               "\', nearest_low_height = ", low_height,
                               " WHERE id = ", id, sep = ""))
      } else {
        dbSendQuery(con, paste("UPDATE surv_pv_cst.tbl_effort SET tide_height = -99",
                               ", nearest_high_height = -99", 
                               ", nearest_low_height = -99", 
                               " WHERE id = ", id, sep = ""))
      }
    }
  }
}

# Tide processing for NOAA legacy data (1998-2002)
# dbSendQuery(con,  paste("UPDATE surv_pv_cst.archive_poly_counts_9802 SET tide_height = NULL",
#                        ", nearest_high_dt = NULL",
#                        ", nearest_high_height = NULL",
#                        ", nearest_low_dt = NULL",
#                        ", nearest_low_height = NULL", sep = ""))
leg9802 <- dbGetQuery(con, "SELECT l.id, survey_dt, station 
                   FROM surv_pv_cst.archive_poly_counts_9802 l
                   INNER JOIN surv_pv_cst.geo_polys
                   USING (polyid) 
                   WHERE tide_height IS NULL")
if (nrow(leg9802) > 0) {
  leg9802$tide_dt <- as.POSIXct(leg9802$survey_dt, tz = "America/Vancouver")
  attributes(leg9802$tide_dt)$tzone <- "GMT"
  
  for (i in 1:nrow(leg9802)){
    if (leg9802$survey_dt[i] == '1111-01-01 01:11:11') {
      dbSendQuery(con, paste("UPDATE surv_pv_cst.archive_poly_counts_9802 SET tide_height = -99",
                             ", nearest_high_height = -99", 
                             ", nearest_low_height = -99", 
                             " WHERE id = ", id, sep = ""))
    } else {
      # Set processing variables
      station <- leg9802$station[i]
      tide_dt <- leg9802$tide_dt[i]
      id <- leg9802$id[i]
      
      # Extract tidal data
      height <- get_tide_height(tide_dt, station)
      high <- get_nearest_hi_lo(tide_dt, station, "High")
      low <- get_nearest_hi_lo(tide_dt, station, "Low")
      
      high_height <- high[[1]][1]
      high_dt <- high[[2]][1]
      
      low_height <- low[[1]][1]
      low_dt <- low[[2]][1]
      
      # Update DB
      if (is.na(height) == FALSE & is.na(high_height) == FALSE & is.na(high_dt) == FALSE & is.na(low_height) == FALSE & is.na(low_dt) == FALSE) {
        dbSendQuery(con, paste("UPDATE surv_pv_cst.archive_poly_counts_9802 SET tide_height = ", height,
                               ", nearest_high_dt = \'", high_dt,
                               "\', nearest_high_height = ", high_height,
                               ", nearest_low_dt = \'", low_dt,
                               "\', nearest_low_height = ", low_height,
                               " WHERE id = ", id, sep = ""))
      } else {
        dbSendQuery(con, paste("UPDATE surv_pv_cst.archive_poly_counts_9802 SET tide_height = -99",
                               ", nearest_high_height = -99", 
                               ", nearest_low_height = -99", 
                               " WHERE id = ", id, sep = ""))
      }
    }
  }
}

# Tide processing for NOAA legacy data (1996-1997)
# dbSendQuery(con,  paste("UPDATE surv_pv_cst.archive_poly_counts_9697 SET tide_height = NULL",
#                        ", nearest_high_dt = NULL",
#                        ", nearest_high_height = NULL",
#                        ", nearest_low_dt = NULL",
#                        ", nearest_low_height = NULL", sep = ""))
leg9697 <- dbGetQuery(con, "SELECT l.id, survey_dt, station 
                   FROM surv_pv_cst.archive_poly_counts_9697 l
                   INNER JOIN surv_pv_cst.geo_polys
                   USING (polyid) 
                   WHERE tide_height IS NULL")
if (nrow(leg9697) > 0) {
  leg9697$tide_dt <- as.POSIXct(leg9697$survey_dt, tz = "America/Vancouver")
  attributes(leg9697$tide_dt)$tzone <- "GMT"
  
  for (i in 1:nrow(leg9697)){
    if (leg9697$survey_dt[i] == '1111-01-01 01:11:11') {
      dbSendQuery(con, paste("UPDATE surv_pv_cst.archive_poly_counts_9697 SET tide_height = -99",
                             ", nearest_high_height = -99", 
                             ", nearest_low_height = -99", 
                             " WHERE id = ", id, sep = ""))
    } else {
      # Set processing variables
      station <- leg9697$station[i]
      tide_dt <- leg9697$tide_dt[i]
      id <- leg9697$id[i]
      
      # Extract tidal data
      height <- get_tide_height(tide_dt, station)
      high <- get_nearest_hi_lo(tide_dt, station, "High")
      low <- get_nearest_hi_lo(tide_dt, station, "Low")
      
      high_height <- high[[1]][1]
      high_dt <- high[[2]][1]
      
      low_height <- low[[1]][1]
      low_dt <- low[[2]][1]
      
      # Update DB
      if (is.na(height) == FALSE & is.na(high_height) == FALSE & is.na(high_dt) == FALSE & is.na(low_height) == FALSE & is.na(low_dt) == FALSE) {
        dbSendQuery(con, paste("UPDATE surv_pv_cst.archive_poly_counts_9697 SET tide_height = ", height,
                               ", nearest_high_dt = \'", high_dt,
                               "\', nearest_high_height = ", high_height,
                               ", nearest_low_dt = \'", low_dt,
                               "\', nearest_low_height = ", low_height,
                               " WHERE id = ", id, sep = ""))
      } else {
        dbSendQuery(con, paste("UPDATE surv_pv_cst.archive_poly_counts_9697 SET tide_height = -99",
                               ", nearest_high_height = -99", 
                               ", nearest_low_height = -99", 
                               " WHERE id = ", id, sep = ""))
      }
    }
  }
}

# Tide processing for NOAA Pribilof data
# dbSendQuery(con,  paste("UPDATE surv_pv_cst.archive_poly_counts_pribs SET tide_height = NULL",
#                        ", nearest_high_dt = NULL",
#                        ", nearest_high_height = NULL",
#                        ", nearest_low_dt = NULL",
#                        ", nearest_low_height = NULL", sep = ""))
pribs <- dbGetQuery(con, "SELECT l.id, survey_dt, station 
                   FROM surv_pv_cst.archive_poly_counts_pribs l
                   INNER JOIN surv_pv_cst.geo_polys
                   USING (polyid) 
                   WHERE tide_height IS NULL")
if (nrow(pribs) > 0) {
  pribs$tide_dt <- as.POSIXct(pribs$survey_dt, tz = "America/Vancouver")
  attributes(pribs$tide_dt)$tzone <- "GMT"
  
  for (i in 1:nrow(pribs)){
    if (is.na(pribs$survey_dt[i])) {
      dbSendQuery(con, paste("UPDATE surv_pv_cst.archive_poly_counts_pribs SET tide_height = -99",
                             ", nearest_high_height = -99", 
                             ", nearest_low_height = -99", 
                             " WHERE id = ", pribs$id[i], sep = ""))
    } else {
      # Set processing variables
      station <- pribs$station[i]
      tide_dt <- pribs$tide_dt[i]
      id <- pribs$id[i]
      
      # Extract tidal data
      height <- get_tide_height(tide_dt, station)
      high <- get_nearest_hi_lo(tide_dt, station, "High")
      low <- get_nearest_hi_lo(tide_dt, station, "Low")
      
      high_height <- high[[1]][1]
      high_dt <- high[[2]][1]
      
      low_height <- low[[1]][1]
      low_dt <- low[[2]][1]
      
      # Update DB
      if (is.na(height) == FALSE & is.na(high_height) == FALSE & is.na(high_dt) == FALSE & is.na(low_height) == FALSE & is.na(low_dt) == FALSE) {
        dbSendQuery(con, paste("UPDATE surv_pv_cst.archive_poly_counts_pribs SET tide_height = ", height,
                               ", nearest_high_dt = \'", high_dt,
                               "\', nearest_high_height = ", high_height,
                               ", nearest_low_dt = \'", low_dt,
                               "\', nearest_low_height = ", low_height,
                               " WHERE id = ", pribs$id[i], sep = ""))
      } else {
        dbSendQuery(con, paste("UPDATE surv_pv_cst.archive_poly_counts_pribs SET tide_height = -99",
                               ", nearest_high_height = -99", 
                               ", nearest_low_height = -99", 
                               " WHERE id = ", pribs$id[i], sep = ""))
      }
    }
  }
}

RPostgreSQL::dbDisconnect(con)
rm(con, station, tide_dt, id, height, high, low, high_height, high_dt, low_height, low_dt, i)

