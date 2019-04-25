#!/bin/sh
cd /home/tsadash/tsatool-app
for i in 3 4 5 6 7 8 9 10 11 12
do
  j=$(printf "%02d" "$i")
  fname="data/anturi_arvo-2018_$j.csv"
  python3 "$fname"
  wait
done
python3 insert_to_seobs_per_stationid.py 3
