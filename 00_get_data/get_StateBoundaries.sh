#!bin/bash

#-----------------------------------------------------------------
#----- user inputs -----------------------------------------------
#-----------------------------------------------------------------
# data_dir="/c/Users/kerrie/Documents/02_LocalData/boundaries/orig" # location to download to
data_dir="/e/data/boundaries/orig" # location to download to
filenames=( cb_2023_us_state_500k.zip cb_2023_us_state_5m.zip cb_2023_us_state_20m.zip )
#-----------------------------------------------------------------

for f in "${filenames[@]}"
do
    # download
    curl --create-dirs -O --output-dir $data_dir https://www2.census.gov/geo/tiger/GENZ2023/shp/$f
    # unzip
    unzip -o $data_dir/$f -d $data_dir
    # delete zip file
    rm $data_dir/$f
done

# data landing page 
# https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html

# citation info
# U.S. Census Bureau, “cb_2023_us_state_5m”, Cartographic Boundary Files, 2023, https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html, accessed on September 20, 2024.

# acknowledgement info
# none stated

# downloaded 2024 Sep 20