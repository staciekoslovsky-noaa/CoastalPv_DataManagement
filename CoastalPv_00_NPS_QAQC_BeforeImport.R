# Coastal Pv Surveys: Import data from NPS
# S. Koslovsky

# Set variables --------------------------------------------------
import_file <- "C:/Users/Stacie.Hardy/Work/SMK/Projects/AS_HarborSeal_Coastal/Data/JNW_HarborSealCounts_2018-2023/NPS_Womble_HSeals_2018-2023_JNW_20250318_final.csv"
export_file <- "C:/Users/Stacie.Hardy/Work/SMK/Projects/AS_HarborSeal_Coastal/Data/JNW_HarborSealCounts_2018-2023/NPS_Womble_HSeals_2018-2023_JNW_20250318_final_missingExpectedSites_20250424.csv"

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
install_pkg("dplyr")
install_pkg("lubridate")

# Run code -------------------------------------------------------

# Get reference data from Git
polyids <- read.csv("https://raw.githubusercontent.com/staciekoslovsky-noaa/CoastalPv_DataManagement/main/CoastalPv_00_NPS_Match2PolyID.csv") 
polyids <- read.csv("C:/Users/Stacie.Hardy/Work/SMK/GitHub/CoastalPv_DataManagement/CoastalPv_00_NPS_Match2PolyID.csv") 

# Create NOAA-NPS matching haulout site metadata
haulouts_noaa <- polyids %>%
  filter(haulout != "XX") %>%
  select(polyid, nps_site_name) %>%
  unique() %>%
  mutate(in_noaa = 'Y')

haulouts_noaa <- haulouts_noaa %>%
  select(nps_site_name)

# Read data from CSV and assign trackid
data <- read.csv(import_file) %>%
  mutate(survey_dt = lubridate::mdy_hm(survey_dt, tz = "UTC")) %>%
  mutate(trackid = paste0("JNW_", format(survey_dt, "%Y%m%d"))) %>%
  mutate(survey_dt = survey_dt + 8 * 60 * 60) %>%
  left_join(polyids, by = "nps_site_name") %>%
  mutate(surveyed = ifelse(surveyable == "Y", 1, 0)) %>%
  unique()

tracks <- data %>%
  select(trackid) %>%
  unique()

# Create tracks:sites list of expected data
missing_expected <- tracks %>%
  cross_join(haulouts_noaa) %>%
  left_join(data %>% select(trackid, nps_site_name, survey_dt), by = c("trackid", "nps_site_name")) %>%
  filter(is.na(survey_dt))

# Export data
write.csv(missing_expected, export_file, quote = FALSE, row.names = FALSE)