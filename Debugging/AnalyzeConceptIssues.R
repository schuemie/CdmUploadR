# Code for analyzing issues during ETL
library(DatabaseConnector)

logFileName <- "D:/Synpuf/EtlOutput/concept_debug_log.txt"

connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(keyring::key_get("postgresServer"), keyring::key_get("postgresDatabase"), sep = "/"),
                                             user = keyring::key_get("postgresUser"),
                                             password = keyring::key_get("postgresPassword"),
                                             port = keyring::key_get("postgresPort"))

cdmDatabaseSchema <- "synpuf"


log <- read.table(logFileName, sep = "\t", blank.lines.skip = TRUE, comment.char = "|", quote = "|")
messageTypes <- unique(gsub(" [0-9]+: $", "", log$V1))
messageTypes

# No self map from OMOP (HCPCS/CPT4) ----------------
missingSelfMap <- log[grepl("^No self map from OMOP \\(HCPCS/CPT4\\)", log$V1), ]
connection <- connect(connectionDetails)
sql <- "
SELECT TOP 100 * 
FROM @cdm_database_schema.procedure_occurrence 
WHERE procedure_source_concept_id IN (@concept_ids);
"
results <- renderTranslateQuerySql(connection = connection, 
                                   sql = sql,
                                   cdm_database_schema = cdmDatabaseSchema,
                                   concept_ids = missingSelfMap$V2[1:10])
disconnect(connection)

# No map from OMOP (NCD) to OMOP (RxNorm) ---------------------------------
missingNdcMap <- log[grepl("^No map from OMOP \\(NCD\\) to OMOP \\(RxNorm\\)", log$V1), ]
missingDrugNames <- missingNdcMap$V3

missingDrugNames[grepl("Hydroxychloroquine", missingDrugNames, ignore.case = TRUE)]

missingDrugNames[grepl("Amoxicillin", missingDrugNames, ignore.case = TRUE)]

missingDrugNames[grepl("Azithromycin", missingDrugNames, ignore.case = TRUE)]

missingDrugNames[grepl("Sulfadiazine", missingDrugNames, ignore.case = TRUE)]

connection <- connect(connectionDetails)
sql <- "
SELECT COUNT(*) 
FROM @cdm_database_schema.drug_era
WHERE drug_concept_id = (
  SELECT concept_id
  FROM @cdm_database_schema.concept
  WHERE standard_concept = 'S'
    AND concept_class_id = 'Ingredient'
    AND concept_name = '@concept_name'
);
"
renderTranslateQuerySql(connection = connection, 
                        sql = sql,
                        cdm_database_schema = cdmDatabaseSchema,
                        concept_name = 'hydroxychloroquine')
# 29864

renderTranslateQuerySql(connection = connection, 
                        sql = sql,
                        cdm_database_schema = cdmDatabaseSchema,
                        concept_name = 'amoxicillin')
# 586959

renderTranslateQuerySql(connection = connection, 
                        sql = sql,
                        cdm_database_schema = cdmDatabaseSchema,
                        concept_name = 'azithromycin')
# 91432


renderTranslateQuerySql(connection = connection, 
                        sql = sql,
                        cdm_database_schema = cdmDatabaseSchema,
                        concept_name = 'sulfadiazine')
# 12873

disconnect(connection)


# Check Hydroxychloroquine cohort --------------------------------

connection <- connect(connectionDetails)
sql <- "
SELECT * 
FROM @cdm_database_schema.drug_exposure
INNER JOIN @cdm_database_schema.concept_ancestor
  ON drug_concept_id = descendant_concept_id
WHERE ancestor_concept_id = @concept_id
ORDER BY person_id;
"
drugs <- renderTranslateQuerySql(connection = connection, 
                                 sql = sql,
                                 cdm_database_schema = cdmDatabaseSchema,
                                 concept_id = 1777087) # Hydroxychloroquine
nrow(drugs)
# [1] 30829

head(drugs$PERSON_ID)
# [1] 208 230 240 258 294 533

sql <- "
SELECT * 
FROM @cdm_database_schema.condition_occurrence
INNER JOIN @cdm_database_schema.concept_ancestor
  ON condition_concept_id = descendant_concept_id
WHERE ancestor_concept_id = @concept_id
ORDER BY person_id;"
conditions <- renderTranslateQuerySql(connection = connection, 
                                      sql = sql,
                                      cdm_database_schema = cdmDatabaseSchema,
                                      concept_id = 80809) # Rheumatoid arthritis
nrow(conditions)
# [1] 1280566

head(conditions$PERSON_ID)
# [1] 7 7 7 7 7 7

# Overlap:
sum(drugs$PERSON_ID %in% conditions$PERSON_ID)
# [1] 10506

disconnect(connection)

