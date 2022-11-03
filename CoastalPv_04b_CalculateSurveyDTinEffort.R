# Coastal Pv Surveys: Calculate date/time for polygons
# Adapted from Jay's 2008 BumpHunting code
# S. Hardy, 16MAR2018

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

find_peaks <- function(TimeInput, BandW = NA) {
  # Get rid of any missing values
  TimeInput <- TimeInput[!is.na(TimeInput)]
  
  # If all missing values, return dataframe of NAs
  if(length(TimeInput) == 0) return(data.frame(Pk1Time = NA, Pk1Height = NA, Pk2Time = NA, Pk2Height = NA,  Pk3Time = NA, Pk3Height = NA))
  
  # If POSIXct class, change to numeric in fractional minutes, otherwise just use vector as inputted
  if(any(class(TimeInput) == "POSIXct")) {
    TimeVec <- as.POSIXlt(TimeInput)$yday*24*60 +
      as.POSIXlt(TimeInput)$hour*60 +
      as.POSIXlt(TimeInput)$min +
      as.POSIXlt(TimeInput)$sec/60
    BandW <- 10
  } else {
    TimeVec <- as.numeric(TimeInput)
  }
  
  # Start the time vector at 0
  TimeVec <- TimeVec - min(TimeVec)
  
  # Use default bandwidth or specified bandwitch
  if(is.na(BandW)) densout <- density(TimeVec) else
    densout <- density(TimeVec, bw = BandW)
  
  # Length of data vector
  ny <- length(densout$y)
  
  # Find x-values where y goes up then down
  peaks <- densout$y[2:(ny - 1)] - densout$y[1:(ny - 2)] >= 0 & densout$y[2:(ny - 1)] - densout$y[3:ny] >= 0
  
  # Create a data frame of peaks
  df.out <- data.frame(xpeaks = densout$x[2:(ny - 1)][peaks], ypeaks = densout$y[2:(ny - 1)][peaks])
  
  # Get the order of the peaks for sorting
  ord <- order(df.out[, 2], decreasing = TRUE)
  
  # Sort on the peaks
  df.out <- df.out[ord, ]
  
  # Number of peaks
  ndf <- length(df.out[, 1])
  PksOut <- data.frame(Pk1Time = as.POSIXct(NA), Pk1Height = NA, Pk2Time = as.POSIXct(NA), Pk2Height = NA, Pk3Time = as.POSIXct(NA), Pk3Height = NA)
  for(k in 1:ndf) {
    if(k == 1) {
      PksOut[,"Pk1Time"] <- TimeInput[abs(df.out[k, "xpeaks"] - TimeVec) == min(abs(df.out[k, "xpeaks"] - TimeVec))][1]
      PksOut[,"Pk1Height"] <- df.out[k, 2]
    }
    if(k == 2) {
      PksOut[,"Pk2Time"] <- TimeInput[abs(df.out[k, "xpeaks"] - TimeVec) == min(abs(df.out[k, "xpeaks"] - TimeVec))][1]
      PksOut[,"Pk2Height"] <- df.out[k, 2]
    }
    if(k == 3) {
      PksOut[,"Pk3Time"] <- TimeInput[abs(df.out[k, "xpeaks"] - TimeVec) == min(abs(df.out[k, "xpeaks"] - TimeVec))][1]
      PksOut[,"Pk3Height"] <- df.out[k, 2]
    }
  }
  PksOut
}

# Install libraries ----------------------------------------------
install_pkg("RPostgreSQL")

# Run code -------------------------------------------------------
# Connect to PostgreSQL ---------------------------------------------------------------
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_user"), 
                              password = Sys.getenv("user_pw"))

# Assign missing date/times based on counted images ------------------------------------
RPostgreSQL::dbSendQuery(con, "UPDATE surv_pv_cst.tbl_effort b
                         SET survey_dt = calc_dt,
                         survey_dt_source = 'Counted images'
                         FROM surv_pv_cst.proc_datetime_4effort a
                         WHERE survey_dt IS NULL AND a.trackid = b.trackid AND a.polyid = b.polyid AND a.track_rep = b.track_rep")

# Assign missing date/times based on GPS track ----------------------------------------
polys <- RPostgreSQL::dbGetQuery(con, "SELECT * FROM surv_pv_cst.proc_effort_missing_dt")

# Loop through each track and process data
for(i in 1:nrow(polys)) {
  bump <- RPostgreSQL::dbGetQuery(con, paste("SELECT gps_dt FROM surv_pv_cst.geo_track_pts t ",
                                             "INNER JOIN surv_pv_cst.geo_polys p ",
                                             "ON ST_Intersects(t.geom, p.geom) ",
                                             "WHERE trackid = \'", polys$trackid[i], "\' AND ",
                                             "polyid = \'", polys$polyid[i], "\' ",
                                             sep = ""))
  if (nrow(bump) > 0) {
    bump$gps_dt <- as.POSIXct(bump$gps_dt, tz = "America/Vancouver")
    attributes(bump$gps_dt)$tzone <- "GMT"
    peaks <- find_peaks(bump$gps_dt)
    
    # Update DB
    if (polys$num_reps[i] == 1 | polys$track_rep[i] == 1) {
      if (!is.na(peaks$Pk1Time)) {
        RPostgreSQL::dbSendQuery(con, paste("UPDATE surv_pv_cst.tbl_effort SET survey_dt = \'",
                                            peaks$Pk1Time, 
                                            "\', survey_dt_source = \'GPS bumps\' WHERE id = ",
                                            polys$id[i], sep = ""))
      } else {
        RPostgreSQL::dbSendQuery(con, paste("UPDATE surv_pv_cst.tbl_effort SET survey_dt = \'1111-01-01 01:11:11\',",
                                            "survey_dt_source = \'GPS bumps\' WHERE id = ",
                                            polys$id[i], sep = ""))
      }
    } 
    
    if (polys$track_rep[i] == 2) {
      if (!is.na(peaks$Pk2Time)) {
        RPostgreSQL::dbSendQuery(con, paste("UPDATE surv_pv_cst.tbl_effort SET survey_dt = \'",
                                            peaks$Pk2Time, 
                                            "\', survey_dt_source = \'GPS bumps\' WHERE id = ",
                                            polys$id[i], sep = ""))
      } else {
        RPostgreSQL::dbSendQuery(con, paste("UPDATE surv_pv_cst.tbl_effort SET survey_dt = \'1111-01-01 01:11:11\',",
                                            "survey_dt_source = \'GPS bumps\' WHERE id = ",
                                            polys$id[i], sep = ""))
      }
    } 
  } else {
    RPostgreSQL::dbSendQuery(con, paste("UPDATE surv_pv_cst.tbl_effort SET survey_dt = \'1111-01-01 01:11:11\',",
                                        "survey_dt_source = \'GPS bumps\' WHERE id = ",
                                        polys$id[i], sep = ""))
  }
}
RPostgreSQL::dbDisconnect(con)
rm(bump, con, peaks, i, find_peaks, install_pkg)


