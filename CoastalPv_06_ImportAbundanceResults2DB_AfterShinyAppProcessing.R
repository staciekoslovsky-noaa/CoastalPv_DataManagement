# Harbor Seal: import abundance results (generated for Shiny app) to DB for AGOL

## Get packages -----------------------------------------
library(tidyverse)
library(RPostgreSQL)
library(geojsonio)
library(sf)

# Connect to DB and get starting data
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password =  Sys.getenv("admin_pw"))

# Process survey polygons with most recent abundance estimates for DB
url.survey_polygons <- "C://smk/HarborSealApp/4app/survey_polygons.geojson"
survey_polygons <- geojsonio::geojson_read(url.survey_polygons, what = "sp") %>% 
  as.data.frame() %>%
  arrange(polyid) %>%
  mutate(year = 2023) %>%
  select(polyid, year, abund_est, abund_b95, abund_t95, trend_est, trend_b95, trend_t95)

# Write data to DB
RPostgreSQL::dbWriteTable(con, c("surv_pv_cst", "res_abundance_2023"), survey_polygons, append = FALSE, row.names = FALSE)

# Disconnect from DB
RPostgreSQL::dbDisconnect(con)