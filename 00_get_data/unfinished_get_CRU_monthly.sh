#!bin/bash
# YOU NEED A CEDA ACCOUNT TO RUN THIS
# THEN SET ENVIRONMENTAL VARIABLES: CEDA_USERNAME, CEDA_PASSWORD  (e.g. export CEDA_USERNAME="myusername")
# THEN INSTALL PYTHON PACKAGES: cryptography & ContrailOnlineCAClient (this one has to be pip install)
# THEN COPY THIS SCRIPT AS WELL AS simple_file_downloader.py INTO THE DIRECTORY WHERE YOU WANT TO SAVE THE DOWNLOADS
# THEN EDIT THE USER INPUTS BELOW AND RUN SCRIPT FROM YOUR DATA DOWNLOAD DIR
# YOU MAY GET SOME WARNINGS BUT FILES FOR THE DECADES AND VARIABLES REQUESTED SHOULD DOWNLOAD

#-----------------------------------------------------------------
#----- user inputs -----------------------------------------------
#-----------------------------------------------------------------
# files are decadal, dataset starts with 1901-1910
years_start=($(seq 1951 10 2021)) # first year of filename
years_end=($(seq 1960 10 2020))   # end year of filename
years_end+=('2023')  # end of the dataset
vars=( pre tmn tmx ) # variables we want
#-----------------------------------------------------------------

# loop through downloads by variable and decadal filename
for var in "${vars[@]}"
do
    echo "--------------------"
    echo "getting $var files"
    echo "--------------------"
    for i in "${!years_start[@]}"
    do
        python simple_file_downloader.py https://dap.ceda.ac.uk/badc/cru/data/cru_ts/cru_ts_4.08/data/$var/cru_ts4.08.${years_start[i]}.${years_end[i]}.$var.dat.nc.gz
    done
done


# unpack files
files=($(ls -d *.gz))

for f in "${files[@]}"
do
    echo "unzipping $f"
    gunzip $f
done

# data landing page 
# https://catalogue.ceda.ac.uk/uuid/715abce1604a42f396f81db83aeb2a4b/?search_url=%2F%253Fq%253Dcru%2Bts%26sort_by%253Drelevance%26results_per_page%253D20

# citation info
# University of East Anglia Climatic Research Unit; Harris, I.C.; Jones, P.D.; Osborn, T. (2024): CRU TS4.08: Climatic Research Unit (CRU) Time-Series (TS) version 4.08 of high-resolution gridded data of month-by-month variation in climate (Jan. 1901- Dec. 2023). NERC EDS Centre for Environmental Data Analysis, date of citation. https://catalogue.ceda.ac.uk/uuid/715abce1604a42f396f81db83aeb2a4b/

# acknowledgement info
# none stated

# downloaded 2024 Sep 19