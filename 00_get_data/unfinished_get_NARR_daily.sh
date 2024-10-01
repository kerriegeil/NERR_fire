#!bin/bash

#-----------------------------------------------------------------
#----- user inputs -----------------------------------------------
#-----------------------------------------------------------------
# data_dir="/c/Users/kerrie/Documents/02_LocalData/nclimgrid_daily/orig"
data_dir_base="/e/data/NARR"
years=($(seq 1979 2024)) # dataset starts in 1979
months=($(seq -f "%02g" 1 12))
narr_varnames=( acpcp 
#-----------------------------------------------------------------

# echo "${years[@]}"
# echo "${months[@]}"

# loop through downloads by year and month
for var in "${narr_varnames[@]}"
do 
    echo "downloading $var files"
    if $var=='acpcp'
        data_dir=$data_dir_base/daily/prcp/orig
    fi
    if $var=='acpcp'
        data_dir=$data_dir_base/daily/tmax/orig
    fi
    if $var=='acpcp'
        data_dir=$data_dir_base/daily/tmin/orig
    fi
    
    for YYYY in "${years[@]}"
    do
        echo "getting files for $YYYY"
        curl --create-dirs -O --output-dir $data_dir https://downloads.psl.noaa.gov/Datasets/NARR/Dailies/monolevel/$var.$YYYY.nc
        
    done
done

# data landing page 
# https://www.ncei.noaa.gov/products/land-based-station/nclimgrid-daily

# citation info
# Durre, I., M. F. Squires, R. S. Vose, A. Arguez, W. S. Gross, J. R. Rennie, and C. J. Schreck, 2022b: NOAA's nClimGrid-Daily Version 1 – Daily gridded temperature and precipitation for the Contiguous United States since 1951. NOAA National Centers for Environmental Information, since 6 May 2022, https://doi.org/10.25921/c4gt-r169

# acknowledgement info
# none stated

# downloaded 2024 Sep 18