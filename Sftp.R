# This code is for uploading and downloading large files using the OHDSI SFTP server. Requires an account.

keyFile <- "c:/home/keyFiles/study-coordinator-repro"
userName <- "study-coordinator-repro"


# Install required packages
install.packages("remotes")
remotes::install_github("ohdsi/OhdsiSharing")


# Upload vocabulary files (already zipped) -----------------------------
library(OhdsiSharing)
connection <- sftpConnect(userName = userName, privateKeyFileName = keyFile)
sftpPutFile(connection, "D:/Synpuf/Vocab/Vocab.zip")
sftpDisconnect(connection)

# Upload ETL-ed files (already zipped) --------------------------------
library(OhdsiSharing)
connection <- sftpConnect(userName = userName, privateKeyFileName = keyFile)
sftpPutFile(connection, "D:/Synpuf/EtlOutput.zip")
sftpDisconnect(connection)



# Download vocabulary files ---------------------------------------------
library(OhdsiSharing)
connection <- sftpConnect(userName = userName, privateKeyFileName = keyFile)
sftpGetFiles(connection, "Vocab.zip")
sftpDisconnect(connection)

# Download ETL-ed files ---------------------------------------------
library(OhdsiSharing)
connection <- sftpConnect(userName = userName, privateKeyFileName = keyFile)
sftpGetFiles(connection, "EtlOutput.zip")
sftpDisconnect(connection)