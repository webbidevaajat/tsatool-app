#' # Prepare station and sensor data for tsa database insertion
#' 
#' This script reads `anturi_arvo` and `tiesaa_mittatieto` csv files
#' converts the IDs from LOTJU ids to TSA ids ("ID" -> "VANHA_ID")
#' and saves the results in csv files that are ready
#' to be inserted to the `statobs` and `seobs` tables
#' e.g. with `psql \copy` command.
#' 
#' Give the file paths or URLs to read from as parameters,
#' `anturi_arvo` file first and then `tiesaa_mittatieto` file.
#' 
#' You MUST have available the files that contain the id mappings:
#' 
#' - `data/laskennallinen_anturi.csv`
#' - `data/tiesaa_asema.csv`
#' 
#' Result files are saved under the `data/` directory:
#' 
#' - `data/seobs.csv`
#' - `data/statobs.csv`
#' 
#' Existing files with same names are overwritten.
#' This means you can run this script with one month,
#' copy results to database and then run for the next month,
#' such that the prepared csv files do not pile up in the directory.
#' 
#' File paths are relative to the directory you are in when
#' you run this script.
#' 
#' This has been tested with 2018 dump files.
#' tiesaa_mittatieto files are 200 > MB in size and
#' anturi_arvo files > 6 GB. `fread` reads the entire
#' contents to memory; this means you should have at least,
#' say, 8 GB RAM available to run the script smoothly.
#' 
#' Made with R version 3.6.
#' Install the `data.table` package as follows:
#' `install.packages("data.table")`.
#' 
#' Example usage:
#' 
#' ```
#' Rscript --vanilla convert_dumps.R data/anturi_arvo-2018_01.csv tiesaa_mittatieto-2018_01.csv
#' 
#' # Or with an URL:
#' Rscript --vanilla convert_dumps.R https://my_s3_buck.et/anturi_arvo-2018_01.csv https://my_s3_buck.et/tiesaa_mittatieto-2018_01.csv
#' ```

library(data.table)

getwd() # Check this if the file paths are not working

args <- commandArgs(trailingOnly=TRUE)
stopifnot(length(args) == 2)

anturi_data_file <- args[[1]]
tiesaa_data_file <- args[[2]]

anturi_id_file <- 'data/laskennallinen_anturi.csv'
tiesaa_id_file <- 'data/tiesaa_asema.csv'

stopifnot(dir.exists('data'))
stopifnot(file.exists(anturi_id_file))
stopifnot(file.exists(tiesaa_id_file))

seobs_out_file <- 'data/seobs.csv'
statobs_out_file <- 'data/statobs.csv'

# Assuming cols: 
# ID | ANTURI_ID | ARVO | MITTATIETO_ID | TIEDOSTO_ID
# We only need the first 4.
anturi <- fread(anturi_data_file, select = 1:4)
sprintf("%01d rows read in from %s", nrow(anturi), anturi_data_file)
stopifnot(all(colnames(anturi) == c('ID', 'ANTURI_ID', 'ARVO', 'MITTATIETO_ID')))
a_pairs <- fread(anturi_id_file, select = c('ID', 'VANHA_ID'))
setkey(anturi, ANTURI_ID)
setkey(a_pairs, ID)
# This join drops any rows where ANTURI_ID does not have a match in laskennallinen_anturi.csv.
# Furthermore, we set the col names that are used in the tsa database.
seobs <- a_pairs[anturi, nomatch = 0L][, .(id = i.ID, obsid = MITTATIETO_ID, seid = VANHA_ID, seval = ARVO)]

head(seobs)
tail(seobs)
fwrite(seobs, file = seobs_out_file, sep = ',', col.names = TRUE)
sprintf("%01d rows written out to %s", nrow(seobs), seobs_out_file)

# Assuming cols:
# ID | AIKA | ASEMA_ID
# All are used here.
tiesaa <- fread(tiesaa_data_file)
sprintf("%01d rows read in from %s", nrow(tiesaa), tiesaa_data_file)
stopifnot(all(colnames(tiesaa) == c('ID', 'AIKA', 'ASEMA_ID')))
t_pairs <- fread(tiesaa_id_file, select = c('ID', 'VANHA_ID'))
setkey(tiesaa, ASEMA_ID)
setkey(t_pairs, ID)
# Similar join procedure but now with ASEMA_ID
statobs <- t_pairs[tiesaa, nomatch = 0L][, .(id = i.ID, tfrom = AIKA, statid = VANHA_ID)]
# Timestamp string reformatting: raw data uses Finnish timestamps (assumed),
# we want to explicitly produce UTC timestamps
statobs[, tfrom := gsub(",.*", ".", tfrom)]
statobs[, tfrom := as.POSIXct(tfrom, format = "%d.%m.%Y %H:%M:%S", tz = "Europe/Helsinki")]
statobs[, tfrom := format(tfrom, "%d.%m.%Y %H:%M:%S%z")]
# Check that results look good
head(statobs)
tail(statobs)
fwrite(statobs, file = statobs_out_file, sep = ',', col.names = TRUE)
sprintf("%01d rows written out to %s", nrow(statobs), statobs_out_file)

