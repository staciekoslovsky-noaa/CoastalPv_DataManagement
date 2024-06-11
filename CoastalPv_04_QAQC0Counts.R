library(RPostgreSQL)
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password =  Sys.getenv("admin_pw"))

data <-dbGetQuery(con, "select * from surv_pv_cst.summ_count_by_polyid_4analysis_coastal")

data$non_pup[is.na(data$non_pup)] <- 0
data$pup[is.na(data$pup)] <- 0
data$total <- data$non_pup + data$pup

indBad0 <- vector(length = length(unique(data$polyid)))
for(i in 1:length(unique(data$polyid))) {
  dTmp <- data[data$polyid == unique(data$polyid)[i], ]
  if(any(dTmp$total == 0 & dim(dTmp[dTmp$total != 0,])[1] > 3))
    if(mean(dTmp[dTmp$total != 0, 'total']) - 3*sqrt(var(dTmp[dTmp$total != 0, 'total'])) > 0)
      indBad0[i] = TRUE
    }
review <- unique(data$polyid)[indBad0]
rm(data, indBad0, i, dTmp)
