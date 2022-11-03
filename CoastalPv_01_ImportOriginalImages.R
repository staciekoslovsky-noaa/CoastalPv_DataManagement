# Coastal Pv Surveys: Create spatial dataset of images taken during survey
# S. Hardy, 12JUN2017

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
install_pkg("exifr")
install_pkg("dplyr")
install_pkg("lubridate")
#install_pkg("devtools")
#devtools::install_github("paleolimbot/exifr")

# Run code -------------------------------------------------------
# Set initial working directory -------------------------------------------------------
wd <- "//nmfs/akc-nmml/Polar_Imagery/Surveys_HS/Coastal/"
year <- as.list(2022:2022)

# Create list of folders within directory for which images need to be processed -------
dir <- as.character(list.dirs(paste(wd, "Originals/", year[1], sep = ""), full.names = TRUE, recursive = TRUE))
original <- data.frame(path = dir[grep("[A-Z]$", dir)], stringsAsFactors = FALSE)
if(length(year) > 1) {
  for (i in 2:length(year)){
    yr <- year[i]
    dir <- as.character(list.dirs(paste(wd, "Originals/", yr, sep = ""), full.names = TRUE, recursive = TRUE))
  
    original_temp <- data.frame(path = dir, stringsAsFactors = FALSE)
    original <- rbind(original, original_temp)
    rm(original_temp)
  }
}

# Remove extraneous folders that do not need to have images imported into DB
original1 <- data.frame(path = original[grep("Originals/[0-9][0-9][0-9][0-9]/[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/[A-Z][A-Z][A-Z]$", original$path), ], stringsAsFactors = FALSE)
original2 <- data.frame(path = original[grep("Originals/[0-9][0-9][0-9][0-9]/[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]/[A-Z][A-Z][A-Z]/[A-Z][A-Z][A-Z]_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][a-z]$", original$path), ], stringsAsFactors = FALSE)
original <- rbind(original1, original2) 
rm(original1, original2)
                 
# Remove folders that have previously been processed
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))
imported <- dbGetQuery(con, "SELECT source_file, image_name FROM surv_pv_cst.tbl_image_exif")
imported$path <- dirname(imported$source_file)
imported <- unique(imported$path)
imported <- gsub("\\\\", "/", imported)

original$processed <- ifelse(original$path %in% imported, "Yes", "No")
original <- original[which(original$processed == "No"), ]

# Extract exif data from original images and create spatial dataset
tags <- c("SourceFile", "FileName", "FileAccessDate", "DateTimeOriginal",
          "Make", "Model", "Lens", "ExposureTime", "FNumber", "FocalLength", "ISO", "Quality",
          "WhiteBalance", "Sharpness", "FocusMode", "ExifImageWidth", "ExifImageHeight",
          "GPSDateTime", "GPSLatitude", "GPSLongitude", "GPSAltitude"#, "GPSSatellites"
          )

original_exif <- read_exif("//nmfs/akc-nmml/Polar_Imagery/Surveys_HS/Coastal/Originals/test_exif.jpg", tags = tags)
#original_exif$ISO <- as.character(original_exif$ISO)
original_exif <- data.frame(SourceFile = original_exif[0, c(1:2)], stringsAsFactors = FALSE)

for (i in c(1:nrow(original))){
  images <- list.files(original$path[i], pattern = "jpg$|JPG$", full.names = TRUE)
  
  if (length(images) > 0) {
    temp_exif <- exifr::read_exif(images, tags = tags)
    temp_exif <- data.frame(lapply(temp_exif, as.character), stringsAsFactors = FALSE)

    # Check for missing columns
    missing <- setdiff(tags, names(temp_exif)) 
    temp_exif[missing] <- ''                   
    temp_exif <- temp_exif[tags]
    
    # Merge with other exif data
    original_exif <- bind_rows(original_exif, temp_exif)
    rm(temp_exif)
  }
}

original_exif$GPSSatellites <- ""

# Remove extraneous records
original_exif$image_name <- toupper(original_exif$FileName)
original_exif$photog_track_id <- substr(original_exif$image_name, 1, 12)
original_exif <- subset(original_exif, grepl("[A-z][A-z][A-z]_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]", original_exif$photog_track_id))

# Assign unique ID to each record
next_id <- as.integer(dbGetQuery(con, "SELECT max(id) FROM surv_pv_cst.tbl_image_exif"))
row.names(original_exif) <- 1:nrow(original_exif) 
original_exif$id <- as.integer(row.names(original_exif)) + as.integer(next_id)
original_exif$GPSSatellites <- ''

# Process fields for import
original_exif <- original_exif[, c("id", "SourceFile", "FileAccessDate", "image_name", "photog_track_id", "DateTimeOriginal", 
                                   "Make", "Model", "Lens", "ExposureTime", "FNumber", "FocalLength", "ISO", "Quality", "WhiteBalance", "Sharpness", "FocusMode", "ExifImageWidth", "ExifImageHeight",
                                   "GPSDateTime", "GPSLatitude", "GPSLongitude", "GPSAltitude", "GPSSatellites")]
original_exif[original_exif == ""] <- NA 
substr(original_exif$FileAccessDate, 5, 5) <- "-"
substr(original_exif$FileAccessDate, 8, 8) <- "-"
original_exif$FileAccessDate <- ymd_hms(original_exif$FileAccessDate, tz = "America/Vancouver")
original_exif$DateTimeOriginal <- ifelse(original_exif$DateTimeOriginal == "                   ", NA, original_exif$DateTimeOriginal)
original_exif$DateTimeOriginal <- ifelse(substr(original_exif$DateTimeOriginal, 8, 8) == ":", 
                                         original_exif$DateTimeOriginal, 
                                         paste(substr(original_exif$DateTimeOriginal, 1, 5), "0", substr(original_exif$DateTimeOriginal, 6, nchar(original_exif$DateTimeOriginal)), sep = ""))
original_exif$DateTimeOriginal <- ifelse(substr(original_exif$DateTimeOriginal, 11, 11) == " ", 
                                         original_exif$DateTimeOriginal, 
                                         paste(substr(original_exif$DateTimeOriginal, 1, 8), "0", substr(original_exif$DateTimeOriginal, 9, nchar(original_exif$DateTimeOriginal)), sep = ""))
original_exif$DateTimeOriginal <- ifelse(substr(original_exif$DateTimeOriginal, 14, 14) == ":", 
                                         original_exif$DateTimeOriginal, 
                                         paste(substr(original_exif$DateTimeOriginal, 1, 11), "0", substr(original_exif$DateTimeOriginal, 12, nchar(original_exif$DateTimeOriginal)), sep = ""))
original_exif$DateTimeOriginal <- ifelse(nchar(original_exif$DateTimeOriginal) == 19, 
                                         original_exif$DateTimeOriginal, 
                                         paste(substr(original_exif$DateTimeOriginal, 1, 17), "0", substr(original_exif$DateTimeOriginal, 18, nchar(original_exif$DateTimeOriginal)), sep = ""))
substr(original_exif$DateTimeOriginal, 5, 5) <- "-"
substr(original_exif$DateTimeOriginal, 8, 8) <- "-"
original_exif$DateTimeOriginal <- format(ymd_hms(original_exif$DateTimeOriginal, tz = "America/Anchorage"), tz = "UTC")
original_exif$Lens <- ifelse(original_exif$Lens == "0 0 0 0" | original_exif$Lens == "0.0 mm f/0.0" , NA, original_exif$Lens)
original_exif$ExposureTime <- ifelse(original_exif$ExposureTime == "0", NA, original_exif$ExposureTime)
original_exif$FNumber <- ifelse(original_exif$FNumber == "0", NA, original_exif$FNumber)
original_exif$FocalLength <- ifelse(original_exif$FocalLength == "0", NA, original_exif$FocalLength)
original_exif$Quality <- gsub(" ", "", original_exif$Quality)
original_exif$WhiteBalance <- gsub(" ", "", original_exif$WhiteBalance)
original_exif$WhiteBalance <- ifelse(original_exif$WhiteBalance == "0", NA, original_exif$WhiteBalance)
original_exif$FocusMode <- gsub(" ", "", original_exif$FocusMode)
original_exif$GPSDateTime <- ifelse(nchar(original_exif$GPSDateTime) == 9, 
       paste(substr(original_exif$DateTimeOriginal, 1, 10), original_exif$GPSDateTime, sep = " "),
       original_exif$GPSDateTime)
substr(original_exif$GPSDateTime, 5, 5) <- "-"
substr(original_exif$GPSDateTime, 8, 8) <- "-"
original_exif$GPSDateTime <- ymd_hms(original_exif$GPSDateTime, tz = "UTC")
original_exif$GPSSatellites <- as.numeric(original_exif$GPSSatellites)
original_exif$GPSLatitude <- ifelse(is.na(original_exif$GPSLatitude), 0, original_exif$GPSLatitude)
original_exif$GPSLatitude <- ifelse(!is.na(original_exif$GPSSatellites) & original_exif$GPSSatellites == 2, 0, original_exif$GPSLatitude)
original_exif$GPSLongitude <- ifelse(is.na(original_exif$GPSLongitude), 0, original_exif$GPSLongitude)
original_exif$GPSLongitude <- ifelse(!is.na(original_exif$GPSSatellites) & original_exif$GPSSatellites == 2, 0, original_exif$GPSLongitude)
original_exif$interpolated_lku <- ifelse(is.na(original_exif$GPSDateTime), "X",
                                         ifelse(original_exif$GPSLatitude == 0, "X",
                                                ifelse(original_exif$GPSLongitude == 0, "X", "N")))
original_exif$use_for_count_lku <- "X"
original_exif$geom <- "0101000020E610000000000000000000000000000000000000"
original_exif$polyid <- ""

# Export data to PostgreSQL -----------------------------------------------------------
colnames(original_exif) <- c("id", "source_file", "exif_extract_dt", "image_name", "photog_date_id", "original_dt", "camera_make", "camera_model",
                             "lens_specs", "exposure_time_sec", "f_stop", "focal_length", "iso", "quality", "white_balance", "sharpness", "focus_mode",
                             "image_width_pix", "image_height_pix", "gps_dt", "latitude", "longitude", "altitude", "gps_satellites", 
                             "interpolated_lku", "use_for_count_lku", "geom", "polyid")
dbWriteTable(con, c("surv_pv_cst", "tbl_image_exif"), original_exif, append = TRUE, row.names = FALSE)
dbSendQuery(con, "UPDATE surv_pv_cst.tbl_image_exif SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)")
dbSendQuery(con, "UPDATE surv_pv_cst.tbl_image_exif SET polyid = NULL WHERE polyid = \'\'")
dbSendQuery(con, "UPDATE surv_pv_cst.tbl_image_exif SET photog_date_id = substr(image_name, 1, 13) WHERE substr(image_name, 13, 1) <> '_'")

# Disconnect for database and delete unnecessary variables ----------------------------
dbDisconnect(con)
rm(con, dir, i, wd, year, yr, next_id)
