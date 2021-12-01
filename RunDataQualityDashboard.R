# (Optional): Code to run DataQualityDashboard on the uploaded CMD data

outputFolder <- "D:/Synpuf/DQD"


# Install DataQualityDashboard ---------------------------------------
install.packages("remotes")
remotes::install_github("ohdsi/DataQualityDashboard", ref = "v5.2.2-fix")


# Run DataQualityDashboard ----------------------------------------------
library(DatabaseConnector)
library(DataQualityDashboard)

connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(keyring::key_get("postgresServer"), keyring::key_get("postgresDatabase"), sep = "/"),
                                             user = keyring::key_get("postgresUser"),
                                             password = keyring::key_get("postgresPassword"),
                                             port = keyring::key_get("postgresPort"))

cdmDatabaseSchema <- "synpuf"
resultsDatabaseSchema <- "ohdsi_results"

executeDqChecks(connectionDetails = connectionDetails, 
                cdmDatabaseSchema = cdmDatabaseSchema, 
                resultsDatabaseSchema = resultsDatabaseSchema,
                cdmSourceName = "SynPuf", 
                cdmVersion = "5.2.2",
                outputFolder = outputFolder)

# View the dashboard --------------------------------------------------
jsonFile <- list.files(outputFolder, ".json", full.names = TRUE)
viewDqDashboard(jsonPath = jsonFile)
