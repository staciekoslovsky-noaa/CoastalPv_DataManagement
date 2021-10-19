library(RCurl)
library(XML)
library(rlist)
library(RPostgreSQL)
library(sf)
library(dplyr)    

# Connect to the DB
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              rstudioapi::askForPassword(paste("Enter your DB password for user account: ", Sys.getenv("pep_admin"), sep = "")))

# Get tide station names from website
theurl <- "http://www.flaterco.com/xtide/locations.html"
webpage <- getURL(theurl)
tables <- readHTMLTable(theurl)
tables <- list.clean(tables, fun = is.null, recursive = FALSE)
n.rows <- unlist(lapply(tables, function(t) dim(t)[1]))
tideW <- tables[[which.max(n.rows)]]
tideW$source <- 'Website'
tideW <- tideW[, c(2, 4:6)]
tideW$Latitude <- as.numeric(ifelse(grepl('° N', tideW$Latitude), 
                          gsub('° N', '', tideW$Latitude),
                          as.numeric(gsub('° S', '', tideW$Latitude)) * (-1)
                          ))
tideW$Longitude <- as.numeric(ifelse(grepl('° E', tideW$Longitude), 
                          gsub('° E', '', tideW$Longitude),
                          as.numeric(gsub('° W', '', tideW$Longitude)) * (-1)
                          ))
tideW <- tideW[grep("Alaska", tideW$Name), ]
rm(theurl, webpage, tables, n.rows)

tideW <- tideW[, c(1:3)]
colnames(tideW) <- c("station", "latitude", "longitude")
tideW$id <- c(1:nrow(tideW))
tideW$geom <- "0101000020E610000000000000000000000000000000000000"
tideW <- tideW[, c("id", "station", "latitude", "longitude", "geom")]
dbWriteTable(con, c("surv_pv_cst", "geo_tidal_stations"), tideW, overwrite = TRUE, row.names = FALSE)
dbSendQuery(con, "UPDATE surv_pv_cst.geo_tidal_stations SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)")
tideW$source <- "Website"

# Get the tide stations used in DB
tideDB <- dbGetQuery(con, "SELECT DISTINCT station FROM surv_pv_cst.geo_polys")
tideDB$source <- 'DB'

# Merge and compare missing
missing <- merge(tideW, tideDB, by = "station", all.y = TRUE)
missing <- missing[which(is.na(missing$source.x)), ]
#rm(missing, tideDB)

# Process nearest tidal station for polyids missing information
#nearPostGIS <- dbGetQuery(con, "SELECT a.polyid, a.station AS poly_station, b.station AS tide_station, ST_Distance(ST_Transform(a.geom, 3338), ST_Transform(b.geom, 3338))/1000 AS dist_km FROM surv_pv_cst.geo_polys AS a, surv_pv_cst.geo_tidal_stations AS b")

tideW.sf <- dbGetQuery(con, "SELECT station, latitude, longitude FROM surv_pv_cst.geo_tidal_stations")
tideW.sf <- sf::st_as_sf(tideW.sf, coords = c("longitude", "latitude"), crs = 4326)
tideW.sf <- sf::st_transform(tideW.sf, crs = 3338)
#sf::write_sf(tideW.sf, "tide.shp")
tideW.sf$id <- 1:nrow(tideW.sf)
tideW.m <- sf::st_coordinates(tideW.sf)
tideW.sf <- as.data.frame(tideW.sf)

tideDB.sf <- dbGetQuery(con, "SELECT polyid, latitude, longitude FROM surv_pv_cst.geo_polys_midpt WHERE polyid IN (SELECT polyid FROM surv_pv_cst.geo_polys WHERE station IS NULL)")
tideDB.sf <- sf::st_as_sf(tideDB.sf, coords = c("longitude", "latitude"), crs = 4326)
tideDB.sf <- sf::st_transform(tideDB.sf, crs = 3338)
tideDB.sf$id <- 1:nrow(tideDB.sf)
tideDB.m <- sf::st_coordinates(tideDB.sf)
tideDB.sf <- as.data.frame(tideDB.sf)

near <- as.data.frame(nabor::knn(tideW.m, tideDB.m, k = 1))
near$id <- 1:nrow(near)
update <- merge(tideDB.sf, near, by = "id")
update <- merge(update, tideW.sf, by.x = "nn.idx", by.y = "id", all.x = TRUE)
update <- update[, c("polyid", "station", "nn.dists")]
colnames(update)[[3]] <- "dist_km"
update$dist_km <- update$dist_km/1000
updateDB <- update[which(update$dist_km <= 30), ]
reviewGIS <- update[which(update$dist_km > 30), ]

for (i in 1:nrow(updateDB)){
  polyid <- updateDB$polyid[i]
  station <- updateDB$station[i]
  dist_km <- updateDB$dist_km[i]
  dbSendQuery(con, paste("UPDATE surv_pv_cst.geo_polys SET station = \'", station, "\', distance_km = ",
                         dist_km, " WHERE polyid = \'", polyid, "\'", sep = ""))
}

#review <- nearest[which(nearest$poly_station != nearest$tide_station), ]

dbSendQuery(con, "DROP surv_pv_cst.geo_tidal_stations")
dbDisconnect(con)
rm(con)
