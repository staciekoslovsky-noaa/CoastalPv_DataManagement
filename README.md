# Coastal Harbor Seal Survey Data Management

This repository stores the code associated with managing coastal harbor seal survey data. Code numbered 0+ are intended to be run sequentially as the data are available for processing. Code numbered 99 are stored for longetivity, but are intended to only be run once to address a specific issue or run as needed, depending on the intent of the code.

The data management processing code is as follows:
* **CoastalPv_00_IdentifyPriorityPolys_YYYY-MM.txt** - code to identify the priority polygons for particular survey efforts (which are denoted by the YYYY-MM in the file name) and create views in the DB of the selected polygons; this code is to be run in PGAdmin
* **CoastalPv_01_ImportEffort.txt** - code for updating the effort data after it has been imported into the DB; this code is to be run in PGAdmin
* *CoastalPv_01_ImportGPXTracks.R*** - code for importing GPS tracklines from survey flights into the DB
* **CoastalPv_01_ImportOriginalImages.R** - code for importing image and exif data into the DB
* **CoastalPv_02_InterpolateCoordsWhenMissing_BulkProcessing.R** - code for getting coordinates for images based on the GPS trackline when coordinates are missing, bad or otherwise needing to be replaced; this code cannot be run until the images and GPS tracklines have been imported into the DB
* **CoastalPv_03a_UpdatePOLYID.txt** - code for assigning images that were counted to polyIDs; this code is to be run in PGAdmin; must be run before tidal covariates are calculated
* **CoastalPv_03b_CalculateSurveyDTinEffort.R** - code for assigning a survey date/time to a polyID in the effort table; this is done either by using the available data or by estimating it with GPS trackline
* **CoastalPv_03c_CalculateTideCovariates.R** - based on the survey date/time generated from the previous step, code to extract high and low tide information for the surveyed polygons
* **CoastalPv_04_QAQC0Counts.R** - code for quality checking 0 counts in the database; this should be run after the counts for a particular year have been completed and before data are provided to statisticians for analysis
* **CoastalPv_05_ExportHarborSealCountsInclGlacial_4Analysis.R** - code for exporting data to CSV format for abundance analyses; exports both coastal and glacial data from the DB

Other code in the repository includes:
* Code to create a new haulout layer (if/when changes have been made):
	* CoastalPv_99_CreateNewHauloutLayer_WhenChangesMade.txt (code to be run in PGAdmin)
* Code to summarize the number of seals that were disturbed during survey:
	* CoastalPv_99_SummarizeNumberSealsDisturbed.txt (code to be run in PGAdmin)
* Code for reviewing and updating tidal stations assigned to survey polygons:
	* CoastalPv_99_Tides_UpdateStation.R
	* CoastalPv_99_Tides_UpdateStationDistance.txt
* Code for updating tbl_effort geom field (if/when changes have been made):
	* CoastalPv_99_UpdateEffortGeom_WhenChangesMade.txt
	
This repository is a scientific product and is not official communication of the National Oceanic and Atmospheric Administration, or the United States Department of Commerce. All NOAA GitHub project code is provided on an ‘as is’ basis and the user assumes responsibility for its use. Any claims against the Department of Commerce or Department of Commerce bureaus stemming from the use of this GitHub project will be governed by all applicable Federal law. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by the Department of Commerce. The Department of Commerce seal and logo, or the seal and logo of a DOC bureau, shall not be used in any manner to imply endorsement of any commercial product or activity by DOC or the United States Government.