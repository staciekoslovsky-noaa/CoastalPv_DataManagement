# Coastal Pv Surveys: Import data from NPS
# S. Koslovsky

# Set variables --------------------------------------------------
import_file <- "C:/Users/Stacie.Hardy/Work/SMK/Projects/AS_HarborSeal_Coastal/Data/JNW_HarborSealCounts_2018-2023/NPS_Womble_HSeals_2018-2023_JNW_20250318_final.csv"

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

"%notin%" <- Negate("%in%")

# Install libraries ----------------------------------------------
install_pkg("RPostgreSQL")
install_pkg("dplyr")
install_pkg("lubridate")

# Run code -------------------------------------------------------
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))

RPostgreSQL::dbSendQuery(con, "DELETE FROM surv_pv_cst.archive_poly_counts_nps")

# Get reference data from Git
polyids <- read.csv("https://raw.githubusercontent.com/staciekoslovsky-noaa/CoastalPv_DataManagement/main/CoastalPv_00_NPS_Match2PolyID.csv") 
polyids <- read.csv("C:/Users/Stacie.Hardy/Work/SMK/GitHub/CoastalPv_DataManagement/CoastalPv_00_NPS_Match2PolyID.csv") 

# Create NOAA-NPS matching haulout site metadata
haulouts_noaa <- polyids %>%
  filter(haulout != "XX") %>%
  select(polyid, nps_site_name) %>%
  unique() %>%
  mutate(in_noaa = 'Y')

haulout_noaa_counts <- haulouts_noaa %>%
  select(polyid) %>%
  group_by(polyid) %>%
  mutate(haulout = 1) %>%
  mutate(num_haulouts = sum(haulout)) %>%
  select(polyid, num_haulouts) %>%
  unique()

# Create NPS-only haulout site metadata

haulouts_nps <- polyids %>%
  filter(haulout == "XX") %>%
  filter(polyid %notin% haulouts_noaa$polyid) %>%
  select(polyid, nps_site_name) %>%
  unique()

haulout_nps_counts <- haulouts_nps %>%
  select(polyid) %>%
  group_by(polyid) %>%
  mutate(haulout = 1) %>%
  mutate(num_haulouts = sum(haulout)) %>%
  select(polyid, num_haulouts) %>%
  unique()

haulouts_noaa <- haulouts_noaa %>%
  select(nps_site_name)

haulouts_nps <- haulouts_nps %>%
  select(nps_site_name)

# Read data from CSV and assign trackid
data <- read.csv(import_file) %>%
  mutate(survey_dt = lubridate::mdy_hm(survey_dt, tz = "UTC")) %>%
  mutate(trackid = paste0("JNW_", format(survey_dt, "%Y%m%d"))) %>%
  mutate(survey_dt = survey_dt + 8 * 60 * 60) %>%
  left_join(polyids, by = "nps_site_name") %>%
  mutate(surveyed = ifelse(surveyable == "Y", 1, 0)) %>%
  unique()

# Get count and minimum survey_dt per polyid, trackid
counts <- data %>%
  group_by(trackid, polyid) %>%
  mutate(survey_dt = min(survey_dt)) %>%
  mutate(total_nonpup = sum(count_nonpup)) %>%
  mutate(total_pup = sum(count_pup)) %>%
  select(trackid, polyid, survey_dt, total_nonpup, total_pup) %>%
  unique() %>%
  ungroup()

# Assign effort (Full effort = surveyed 100% of haulouts in polygon)
effort_noaa <- data %>%
  select(trackid, polyid, nps_site_name, surveyed, haulout) %>%
  unique() %>%
  inner_join(haulouts_noaa, by = "nps_site_name") %>%
  
  # group_by(nps_site_name) %>%
  # mutate(surveyed_haulouts = as.integer(sum(surveyed))) %>%
  # select(nps_site_name, surveyed_haulouts) %>%
  # unique()
  
  group_by(trackid, polyid) %>%
  mutate(surveyed_haulouts = as.integer(sum(surveyed))) %>%
  ungroup() %>%
  select(trackid, polyid, surveyed_haulouts) %>%
  unique() %>%
  left_join(haulout_noaa_counts, by = "polyid") %>%
  mutate(haulout_pct = as.numeric(surveyed_haulouts / num_haulouts)) %>%
  mutate(effort = ifelse(haulout_pct == 1, "FS", "PS")) %>%
  select(trackid, polyid, effort)

effort_nps <- data %>%
  select(trackid, polyid, nps_site_name, surveyed, haulout) %>%
  unique() %>%
  inner_join(haulouts_nps, by = "nps_site_name") %>% 
  group_by(trackid, polyid) %>%
  mutate(surveyed_haulouts = as.integer(sum(surveyed))) %>%
  ungroup() %>%
  select(trackid, polyid, surveyed_haulouts) %>%
  unique() %>%
  left_join(haulout_nps_counts, by = "polyid") %>%
  mutate(haulout_pct = as.numeric(surveyed_haulouts / num_haulouts)) %>%
  mutate(effort = ifelse(haulout_pct == 1, "FS", "PS")) %>%
  select(trackid, polyid, effort)

effort <- effort_noaa %>%
  rbind(effort_nps)

# Prepare data for import into DB
next_id <- as.integer(dbGetQuery(con, "SELECT max(id) FROM surv_pv_cst.archive_poly_counts_nps"))
if (is.na(next_id) == TRUE) {next_id <- 0}

import <- counts %>%
  left_join(effort, by = c("trackid", "polyid")) %>%
  mutate(effort = ifelse(is.na(effort), "PS", effort)) %>%
  mutate(id = 1:nrow(counts) + next_id) %>%
  rename(non_pup = total_nonpup,
         pup = total_pup,
         effort_type_lku = effort) %>%
  select(id, trackid, polyid, non_pup, pup, survey_dt, effort_type_lku)

# Load data to DB
RPostgreSQL::dbWriteTable(con, c("surv_pv_cst", "archive_poly_counts_nps"), import, append = TRUE, row.names = FALSE)

# Disconnect for database and delete unnecessary variables ----------------------------
RPostgreSQL::dbDisconnect(con)