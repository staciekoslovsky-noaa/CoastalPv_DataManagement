library(RPostgreSQL)
library(tidyverse)

con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))

# Get coastal data
coastal <-dbGetQuery(con, "SELECT * FROM surv_pv_cst.summ_count_by_polyid_4analysis_coastal") %>%
  mutate(survey_dt = as.POSIXct(survey_dt, tz = "America/Vancouver", format = '%y-%m-%d %h:%m:%s'),
         nearest_high_dt = as.POSIXct(nearest_high_dt, tz = "America/Vancouver", format = '%y-%m-%d %h:%m:%s'),
         nearest_low_dt = as.POSIXct(nearest_low_dt, tz = "America/Vancouver", format = '%y-%m-%d %h:%m:%s'))
         
attributes(coastal$survey_dt)$tzone <- "GMT"
attributes(coastal$nearest_high_dt)$tzone <- "GMT"
attributes(coastal$nearest_low_dt)$tzone <- "GMT"

write.csv(coastal, file = "C:\\\\smk\\CoastalHarborSealCounts_20250327_IncludingNPS.csv", row.names = FALSE)


# Get glacial data
glacial <- dbGetQuery(con, "SELECT * FROM surv_pv_gla.summ_count_by_polyid_4analysis_glacial") %>%
  mutate(survey_dt_gmt = as.POSIXct(survey_dt_gmt, tz = "America/Vancouver"))

attributes(glacial$survey_dt_gmt)$tzone <- "GMT"

write.csv(glacial, file = "C:\\\\smk\\GlacialHarborSealCounts_20250711_addingCOCOA_2020_2021.csv",row.names = FALSE)


# Disconnect from DB
RPostgreSQL::dbDisconnect(con)