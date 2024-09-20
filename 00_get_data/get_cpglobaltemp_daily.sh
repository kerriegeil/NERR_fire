#!bin/bash

#-----------------------------------------------------------------
#----- user inputs -----------------------------------------------
#-----------------------------------------------------------------
data_dir="/c/Users/kerrie/Documents/02_LocalData/CPCunifiedCONUS_daily" # location to download to
years=($(seq 1979 2024)) # dataset starts in 1948
vars=( tmax tmin )
#-----------------------------------------------------------------

# echo "${years[@]}"

# loop through downloads by year
for var in "${vars[@]}"
do
    echo "getting $var files"
    for YYYY in "${years[@]}"
    do
        echo "$YYYY"
        curl --create-dirs -O --output-dir $data_dir https://downloads.psl.noaa.gov/Datasets/cpc_global_temp/$var.$YYYY.nc
    done
done

# data landing page 
# https://psl.noaa.gov/data/gridded/data.cpc.globaltemp.html

# citation info
# none stated

# acknowledgement info
# CPC Global Unified Temperature data provided by the NOAA PSL, Boulder, Colorado, USA, from their website at https://psl.noaa.gov 

# downloaded 2024 Sep 19