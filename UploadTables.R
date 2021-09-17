library(DatabaseConnector)
library(SqlRender)
library(readr)

connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(keyring::key_get("postgresServer"), keyring::key_get("postgresDatabase"), sep = "/"),
                                             user = keyring::key_get("postgresUser"),
                                             password = keyring::key_get("postgresPassword"),
                                             port = keyring::key_get("postgresPort"))

# Use bulk upload? Will be much faster, but requires POSTGRES_PATH variable set to folder containing pg.exe:
bulkLoad <- TRUE

# Schema where the CDM data should be uploaded:
cdmDatabaseSchema <- "synpuf"

# Local folder containing the vocabulary used in the ETL:
vocabFolder <- "D:/Synpuf/Vocab"

# Local folder containing the ETL-ed data. Data is expected to be in CSV files, with names corresponding to 
# the CDM table names. Number prefixes will be ignored. E.g. 'care_site_1.csv will be loaded into the 'case_site' table.
cdmFolder <- "D:/Synpuf/EtlOutput"

# Maximum number of rows that will be loaded into memory before writing to the database:
batchSize <- 1e7



connection <- connect(connectionDetails)

# Create table structures -----------------------------------------
sql <- render("SET SEARCH_PATH = @cdm_database_schema;", cdm_database_schema = cdmDatabaseSchema)
executeSql(connection, sql)

sql <- readSql("OMOP CDM ddl - PostgreSQL.sql")
executeSql(connection, sql)


# Load vocabulary ------------------------------------------------------------------
cdmTables <- tolower(getTableNames(connection, cdmDatabaseSchema))

files <- list.files(vocabFolder, ".csv")
# file <- files[1]
for (file in files) {
  table <- gsub(".csv", "", tolower(file))
  if (table %in% cdmTables) {
    message("Uploading ", file, " to table ", table)
    upload <- function(chunk, pos) {
      message("- Uploading rows " , pos, " to ", (pos + nrow(chunk)))
      dateCols <- grep("_date$", tolower(colnames(chunk)))
      for (dateCol in dateCols) {
        chunk[, dateCol] <- as.Date(as.character(chunk[[dateCol]]), "%Y%m%d")
      }
      conceptIdCols <- grep("concept_id", tolower(colnames(chunk)))
      for (conceptIdCol in conceptIdCols) {
        chunk[, conceptIdCol] <- as.integer(chunk[[conceptIdCol]])
      }
      # For bulk uploading:
      options(encoding = "UTF-8")
      insertTable(connection = connection,
                  databaseSchema = cdmDatabaseSchema,
                  tableName = table,
                  data = chunk,
                  dropTableIfExists = FALSE,
                  createTable = FALSE,
                  bulkLoad = bulkLoad)
    }
    read_delim_chunked(file = file.path(vocabFolder, file), 
                       callback = upload,
                       delim = "\t", 
                       quote = "|", 
                       na = "",
                       col_types = cols(),
                       guess_max = 1e5, 
                       progress = FALSE,
                       chunk_size = batchSize)
  } else {
    message("Skipping file ", file, " because not a CDM table")
  }
}


# Load CDM tables -------------------------------------------------------------------
cdmTables <- tolower(getTableNames(connection, cdmDatabaseSchema))

files <- list.files(cdmFolder, ".csv")
# files <- files[which(files == "person_1.csv"):length(files)]
# file <- files[1]
for (file in files) {
  table <- gsub(".[0-9]+.csv", "", tolower(file))
  if (table %in% cdmTables) {
    message("Uploading ", file, " to table ", table)
    cdmFields <- renderTranslateQuerySql(connection, "SELECT TOP 0 * FROM @cdm_database_schema.@table;", cdm_database_schema = cdmDatabaseSchema, table = table)
    cdmFieldNames <- tolower(colnames(cdmFields))
    integerFields <- cdmFieldNames[sapply(cdmFields, storage.mode) == "integer"]
    
    upload <- function(chunk, pos) {
      message("- Uploading rows " , pos, " to ", (pos + nrow(chunk)))
      mismatchColumns <- colnames(chunk)[!tolower(colnames(chunk)) %in% cdmFieldNames]
      if (length(mismatchColumns) > 0) {
        warning("Ignoring columns not in CDM: ", paste(mismatchColumns, collapse = ","))
        chunk <- chunk[ ,!colnames(chunk) %in% mismatchColumns]
      }
      integerCols <- which(colnames(chunk) %in% integerFields)
      for (integerCol in integerCols) {
        chunk[, integerCol] <- as.integer(chunk[[integerCol]])
      }
      missingDateTimeFields <- cdmFieldNames[grep("_datetime$", cdmFieldNames)]
      missingDateTimeFields <- missingDateTimeFields[!missingDateTimeFields %in% tolower(colnames(chunk))]
      if (length(missingDateTimeFields) > 0) {
        for (field in missingDateTimeFields) {
          column <- gsub("_datetime", "_date", field)
          if (column %in% tolower(colnames(chunk))) {
            message("  Copying field ", column, " to field ", field)
            chunk[field] <- chunk[column]
          } else {
            warning("Could not generate datetime field ", field, " because column ", column, " was not found")
          }
        }
      }
      if (table == "drug_exposure" && any(is.na(chunk$drug_exposure_end_date))) {
        chunk$drug_exposure_end_date <- chunk$drug_exposure_start_date + ifelse(is.na(chunk$days_supply), 0, chunk$days_supply - 1)
      }
      # For bulk uploading:
      options(encoding = "UTF-8")
      insertTable(connection = connection,
                  databaseSchema = cdmDatabaseSchema,
                  tableName = table,
                  data = chunk,
                  dropTableIfExists = FALSE,
                  createTable = FALSE,
                  bulkLoad = bulkLoad)
    }
    
    read_csv_chunked(file = file.path(cdmFolder, file), 
                     callback = upload,
                     na = "",
                     col_types = cols(),
                     guess_max = 1e5, 
                     progress = FALSE,
                     chunk_size = batchSize)
    
    # chunk <-  read_csv(file = file.path(cdmFolder, file),
    #                    col_types = cols(),
    #                    guess_max = 1e5,
    #                    n_max = 1000)
  } else {
    message("Skipping file ", file, " because not a CDM table")
  }
}


# Create indices and constraints ----------------------------------------------------------
sql <- render("SET SEARCH_PATH = @cdm_database_schema;", cdm_database_schema = cdmDatabaseSchema)
executeSql(connection, sql)

sql <- readSql("OMOP CDM constraints - PostgreSQL.sql")
executeSql(connection, sql)

sql <- readSql("OMOP CDM indexes required - PostgreSQL.sql")
executeSql(connection, sql)


# Build eras ------------------------------------------------------------------------
sql <- render("SET SEARCH_PATH = @cdm_database_schema;", cdm_database_schema = cdmDatabaseSchema)
executeSql(connection, sql)

sql <- readSql("buildConditionEras.sql")
renderTranslateExecuteSql(connection, sql)

sql <- readSql("buildDrugEras.sql")
renderTranslateExecuteSql(connection, sql)

# Populate cdm_source --------------------------------------------------
sql <- "SELECT vocabulary_version FROM @cdm_database_schema.vocabulary WHERE vocabulary_id = 'None';"
vocabularyVersion <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                                sql = sql,
                                                                cdm_database_schema = cdmDatabaseSchema)[1, 1]
row <- data.frame(cdm_source_name = "Medicare Claims Synthetic Public Use Files (SynPUFs)",
                  cdm_source_abbreviation = "synPuf",
                  source_description = "Medicare Claims Synthetic Public Use Files (SynPUFs) were created to allow interested parties to gain familiarity using Medicare claims data while protecting beneficiary privacy. These files are intended to promote development of software and applications that utilize files in this format, train researchers on the use and complexities of Centers for Medicare and Medicaid Services (CMS) claims, and support safe data mining innovations. The SynPUFs were created by combining randomized information from multiple unique beneficiaries and changing variable values. This randomization and combining of beneficiary information ensures privacy of health information.",
                  cdm_release_date = Sys.Date(),
                  cdm_version = "5.2.2",
                  vocabulary_version = vocabularyVersion)
insertTable(connection = connection,
            databaseSchema = cdmDatabaseSchema,
            tableName = "cdm_source",
            data = row,
            createTable = FALSE)


disconnect(connection)
