import xarray as xr
import numpy as np
import pandas as pd

# netcdf file locations
pr_file = r'D://data/nclimgrid_daily/prcp_nClimGridDaily_1951-2024_USsouth.nc'
tmax_file = r'D://data/nclimgrid_daily/tmax_nClimGridDaily_1951-2024_USsouth.nc'

# load precip data at a single grid, limit precision per metadata, and convert units
year_start='1951'
year_end='2024'
lat = 32
lon = -90
pr=xr.open_dataset(pr_file).prcp.sel(time=slice(year_start,year_end)).sel(lat=lat,lon=lon, method='nearest').round(2).load()
pr = pr / 25.4
pr_attrs = {'standard_name':'precipitation','long_name':'Precipitation, daily total','units':'inches/day'}
pr.attrs = pr_attrs

# load temperature data at a single grid, limit precision per metadata, and convert units
tmax=xr.open_dataset(tmax_file).tmax.sel(time=slice(year_start,year_end)).sel(lat=lat,lon=lon, method='nearest').round(2).load()
tmax = (tmax *9/5) + 32
tmax_attrs = {'standard_name':'air_temperature','long_name':'Temperature, daily maximum','units':'F'}
tmax.attrs = tmax_attrs

# mean annual precip (inches)
mean_ann_pr = pr.groupby('time.year').sum(min_count=360).mean()

# KBDI
def calc_kbdi(T,PR):
    # determine if the grid is land or water
    landmask=1 if np.isfinite(PR.mean('time')) else 0
    
    # create an integer time index coordinate since datetimes will give us some trouble
    time_index=np.arange(0,len(PR.time)).astype('int')
    PR.coords['time_index']=('time',time_index)
    
    
    # sum precip in 7 day rolling windows
    ndays=7
    pr_thresh=8 # inches
    pr_weeksum=PR.rolling(time=ndays,min_periods=ndays,center=False).sum()
    
    
    # get the first index time where the weekly sum meets the threshold
    # this is index t-1 for the KBDI calc where we'll set it to 0
    day_int = pr_weeksum[pr_weeksum>pr_thresh].isel(time=0).time_index.item()
    
    
    # calculate number of consecutive rain days at each time step
    # I got this code to interrupt a cumulative sum here: https://stackoverflow.com/questions/61753567/convert-cumsum-output-to-binary-array-in-xarray
    rainmask=xr.where(PR>0,1,0) # 1/0 rain/no rain mask
    rr=rainmask.cumsum()-rainmask.cumsum().where(rainmask == 0).ffill(dim='time').fillna(0)
    
    
    # categorize rainfall days: consecutive rain days (2), single rain days (1), no rain days (0)
    # first swap out the time coordinate for the time_index
    rr_swap = rr.swap_dims({'time':'time_index'})
    # label all days that are at least the 3rd consecutive rainfall day with a 5
    cat=xr.where(rr_swap>=3,5,rr_swap)
    # find the indexes of all second consecutive rainfall days
    consec_day2 = np.argwhere(rr_swap.data==2).flatten()
    # find the indexes of all the first consecutive rainfall days
    consec_day1 = consec_day2-1
    # label all consecutive rainfall days with a 2 
    cat[consec_day2]=5 # first put a 5
    cat[consec_day1]=5 # first put a 5
    cat = xr.where(cat==5,2,cat) # convert to 2's
    # should be left with only 2's (consecutive rain days), 1's (single rain days), and 0's (no rain days)
    # np.unique(cat) # change to assert
    
    
    # Calc Pnet 
    acc_thresh = 0.2 # inches
    # Category 0 (no rain days)
    # pnet will be zero where pr is zero
    pnet = PR.swap_dims({'time':'time_index'})
    # Category 1 (single rain days)
    pnet = xr.where(cat==1,pnet-acc_thresh,pnet)
    pnet = xr.where(pnet<0,0,pnet)
    # Category 2 (consecutive rain days) 
    consec_inds = cat[cat==2].time_index
    # initializations
    thresh_flag=False
    end_event=False
    accpr=0. 
    # loop through days in each multi-day rain event
    for i,ind in enumerate(consec_inds): 
        # accumulated precip per rain event
        accpr=accpr+PR[ind].item() 
        # if not over the threshold yet, Pnet is 0
        if accpr<=acc_thresh and not thresh_flag:
            pnet[ind]=0        
        # on the day the threshold is met, subtract the threshold amount and change flag    
        elif accpr>acc_thresh and not thresh_flag:
            accpr=accpr-acc_thresh # accumulate precip and subtract threshold
            pnet[ind]=accpr
            thresh_flag=True        
        # any days after the threshold is met, precip will remain unchanged
        else:
            pnet[ind]=PR[ind].item()     
        # reset accumulation and flag for the next consecutive rain event
        if i != len(consec_inds)-1:
            if (consec_inds[i+1] != consec_inds[i]+1): 
                accpr=0.
                thresh_flag=False
    
    
    # mean annual precip (inches)
    mean_ann_pr = PR.groupby('time.year').sum(min_count=360).mean()
    
    # KBDI initialization
    KBDI = np.full(PR.shape,np.nan) # set all to nan
    Q = KBDI.copy()
    KBDI[day_int]=0   # set to 0 at saturation day t-1
    # print(np.unique(KBDI)) # change to assert
    
    
    # KBDI calculation (inches, Fahrenheit)
    denominator = 1 + 10.88 * np.exp(-0.0441*mean_ann_pr)
    # looping in time, save memory
    for it in range(day_int+1,KBDI.shape[0]):
        Q = max(0,KBDI[it-1] - pnet[it]*100)
        numerator = (800 - Q) * (0.968 * np.exp(0.0486*T[it]) - 8.3)
        KBDI[it] = Q + (numerator/denominator)*1E-3  
        del numerator,Q
    # convert to xarray
    KBDI = xr.DataArray(KBDI, coords = {'time':('time',PR.time.data)})
    KBDI.coords['time_index'] = ('time',time_index)
    return KBDI

test = calc_kbdi(tmax,pr)
test