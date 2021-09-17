CDM Upload Script
=================

This repo contains a single R script for uploading data from local CSV files to a database, using the OMOP Common Data Model (CDM). It assumes the data has already been converted to the CDM, and is just for the uploading part. The script will

- Automatically skip CSV files that do not correspond to a table in the CDM.
- Automatically skip columns that do not correspond to a field in the CDM.
- Automatically add missing datetime fields (by copying date fields).
- Populates the drug_exposure.drug_exposure_end_date field based on the drug_exposure_start_date and days_supply, if not already populated.

The script was designed specifically to upload **Synpuf** data to a **PostgreSQL** database, but could easily be adapted for other data and database platforms.

# Required packages

Use this to install the required packages:

```r
install.packages("SqlRender")
install.packages("DatabaseConnector")
install.packages("readr")
```

It is also recommended to keep your database credentials secure, and not put them in your code. The `keyring` package can help with that:

```r
install.packages("keyring")
```

# Setting credentials in keyring

The script by default uses credentials stored in `keytring`. To set these credentials, use this code, changing the credentials to the correct ones:

```r
keyring::key_set_with_value("postgresServer", password = "mydb.server.com")
keyring::key_set_with_value("postgresDatabase", password = "ohdsi")
keyring::key_set_with_value("postgresUser", password = "john")
keyring::key_set_with_value("postgresPassword", password = "secret!")
keyring::key_set_with_value("postgresPort", password = "5432")
```

This will need to be done only once, after which the credentials will be stored in your system's credential manager.

# Using bulk loading

When using PostgreSQL, it is highly recommended to use bulk loading. This requires:

1. Installing pgAdmin on your local system. You can download pgAdmin from https://www.pgadmin.org/download/.
2. Setting the `POSTGRES_PATH` environmental variable to the folder containing `pg.exe`, e.g.:

```r
Sys.setenv(POSTGRES_PATH = "D:/PostgreSQL/13/bin")
```

# CDM SQL

This repo currently contains SQL for CDM 5.2.2. This can be updated by replacing the SQL files with newer ones from https://github.com/OHDSI/CommonDataModel

# Vocabulary data

The vocabulary data is assumed to have been downloaded from [Athena](https://athena.ohdsi.org/).

# CDM data

The CDM data is assumed to be in CSV files, one or several per CDM table. Number prefixes will be ignored. E.g. `care_site_1.csv` will be loaded into the `case_site` table.