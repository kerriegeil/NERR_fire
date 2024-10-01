#!bin/bash

#-----------------------------------------------------------------
#----- user inputs -----------------------------------------------
#-----------------------------------------------------------------
vars=( tmax tmin )
years=($(seq 1979 2024)) # dataset starts in 1948
# you also need to update the data_dir in the loop below
#-----------------------------------------------------------------

# echo "${years[@]}"

# loop through downloads by year
for var in "${vars[@]}"
do
    data_dir="/e/data/CPCunified/daily/$var/global/orig" # location to download to

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

# downloaded 2024 Sep 23