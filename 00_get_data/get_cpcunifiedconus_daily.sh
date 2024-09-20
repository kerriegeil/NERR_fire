#!bin/bash

#-----------------------------------------------------------------
#----- user inputs -----------------------------------------------
#-----------------------------------------------------------------
data_dir="/c/Users/kerrie/Documents/02_LocalData/CPCunifiedCONUS_daily" # location to download to
years=($(seq 1950 2024)) # dataset starts in 1948
#-----------------------------------------------------------------

# echo "${years[@]}"

# get land mask
curl --create-dirs -O --output-dir $data_dir https://downloads.psl.noaa.gov/Datasets/cpc_us_precip/lsmask.nc

# get monthly precip
curl --create-dirs -O --output-dir $data_dir https://downloads.psl.noaa.gov/Datasets/cpc_us_precip/precip.V1.0.mon.mean.nc

# loop through downloads by year
for YYYY in "${years[@]}"
do
    echo "getting file for $YYYY"
    if [ $YYYY -lt 2007 ]; then
        curl --create-dirs -O --output-dir $data_dir https://downloads.psl.noaa.gov/Datasets/cpc_us_precip/precip.V1.0.$YYYY.nc
    else
        curl --create-dirs -O --output-dir $data_dir https://downloads.psl.noaa.gov/Datasets/cpc_us_precip/RT/precip.V1.0.$YYYY.nc
    done
done

# data landing page 
# https://psl.noaa.gov/data/gridded/data.unified.daily.conus.html

# citation info
# Xie_et_al_2007_JHM_EAG.pdf Xie, P., A. Yatagai, M. Chen, T. Hayasaka, Y. Fukushima, C. Liu, and S. Yang (2007), A gauge-based analysis of daily precipitation over East Asia, J. Hydrometeorol., 8, 607. 626.

# acknowledgement info
# CPC Unified Gauge-Based Analysis of Daily Precipitation over CONUS data provided by the NOAA PSL, Boulder, Colorado, USA, from their website at https://psl.noaa.gov

# downloaded 2024 Sep 19