#!bin/bash

#-----------------------------------------------------------------
#----- user inputs -----------------------------------------------
#-----------------------------------------------------------------
data_dir="/e/data/CPCunified/daily/prcp/global/orig" # location to download to
years=($(seq 1979 2024)) # dataset starts in 1948
# you also need to update the data_dir in the loop below
#-----------------------------------------------------------------

# echo "${years[@]}"

# loop through downloads by year

echo "getting prcp files"
for YYYY in "${years[@]}"
do
    echo "$YYYY"
    curl --create-dirs -O --output-dir $data_dir https://downloads.psl.noaa.gov/Datasets/cpc_global_precip/precip.$YYYY.nc    
done


# data landing page 
# https://psl.noaa.gov/data/gridded/data.cpc.globalprecip.html

# citation info
# (Interpolation algorithm) Xie_et_al_2007_JHM_EAG.pdf Xie, P., A. Yatagai, M. Chen, T. Hayasaka, Y. Fukushima, C. Liu, and S. Yang (2007), A gauge-based analysis of daily precipitation over East Asia, J. Hydrometeorol., 8, 607. 626.
# (Gauge Algorithm Evaluation) Chen_et_al_2008_JGR_Gauge_Algo.pdf Chen, M., W. Shi, P. Xie, V. B. S. Silva, V E. Kousky, R. Wayne Higgins, and J. E. Janowiak (2008), Assessing objective techniques for gauge-based analyses of global daily precipitation, J. Geophys. Res., 113, D04110, doi:10.1029/2007JD009132.

# acknowledgement info
# CPC Global Unified Gauge-Based Analysis of Daily Precipitation data provided by the NOAA PSL, Boulder, Colorado, USA, from their website at https://psl.noaa.gov

# downloaded 2024 Sep 23