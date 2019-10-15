#!/bin/bash
# This script reads tiesaa_asema.csv and laskennallinen_anturi.csv
# files that were included in 2018 LOTJU dump.
# These files contain tens of fields for each station and sensor.
# For the tsa database and interoperability with LOTJU raw data files,
# only id mappings and names are needed.
# NOTE that fixed field positions are assumed!
#
# To use the script, download the original files
# to the script directory.
echo "Filtering tiesaa_asema.csv"
# Field 36: VANHA_ID, 1: ID, 4: NIMI
cat tiesaa_asema.csv | \
  awk --field-separator '|' '{print $36 "|" $1 "|" $4}' | \
  # Only keep lines with correct field structure; headers are left out too
  egrep "^[0-9]+\|[0-9]+\|\".+\"" | \
  sort -t\| -nk1 > tiesaa_asema_filtered.csv
nfrom=$(wc -l tiesaa_asema.csv)
nto=$(wc -l tiesaa_asema_filtered.csv)
echo "Line count: $nfrom --> $nto"
echo "Filtering laskennallinen_anturi.csv"
# Field 10: VANHA_ID, 1: ID, 7: NIMI
cat laskennallinen_anturi.csv | \
  awk --field-separator '|' '{print $10 "|" $1 "|" $7}' | \
  egrep "^[0-9]+\|[0-9]+\|\".+\"" | \
  sort -t\| -nk1 > laskennallinen_anturi_filtered.csv
  nfrom=$(wc -l laskennallinen_anturi.csv)
  nto=$(wc -l laskennallinen_anturi_filtered.csv)
  echo "Line count: $nfrom --> $nto"
