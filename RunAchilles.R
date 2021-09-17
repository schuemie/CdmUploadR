# (Optional): Code to run ACHILLES on the uploaded CMD data

# Install Achilles ---------------------------------------
install.packages("remotes")
remotes::install_github("ohdsi/Achilles")


# Run Achilles ----------------------------------------------
library(DatabaseConnector)
library(Achilles)

connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(keyring::key_get("postgresServer"), keyring::key_get("postgresDatabase"), sep = "/"),
                                             user = keyring::key_get("postgresUser"),
                                             password = keyring::key_get("postgresPassword"),
                                             port = keyring::key_get("postgresPort"))

cdmDatabaseSchema <- "synpuf"
resultsDatabaseSchema <- "ohdsi_results"

achilles(connectionDetails = connectionDetails, 
         cdmDatabaseSchema = cdmDatabaseSchema, 
         resultsDatabaseSchema = resultsDatabaseSchema,
         sourceName = "SynPuf", 
         cdmVersion = "5.2.2")