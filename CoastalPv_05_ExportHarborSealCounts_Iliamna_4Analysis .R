library(RPostgreSQL)
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"),
                              password = Sys.getenv("admin_pw"))

data <-dbGetQuery(con, "select * from surv_pv_cst.summ_count_by_survey_iliamna_4analysis")

data$datetime <- as.POSIXct(data$datetime, tz = "UTC")
attributes(data$datetime)$tzone <- "America/Anchorage"

write.csv(data, file = "C:\\Users\\stacie.hardy\\Work\\SMK\\Projects\\AS_HarborSeal_Coastal\\RFI\\202108_VerHoef_IliamnaHarborSealCounts\\IliamnaHarborSealCounts_20250922.csv",row.names=FALSE)
