import xarray as xr
import numpy as np
import pandas as pd
from more_itertools import consecutive_groups
import dask
import dask.array as da
import matplotlib.pyplot as plt


@jit(nopython=True)
def _calc_pnet_consecutive_numba(pr_data, cat_data, acc_thresh=0.2):
    """Numba-optimized function to calculate Pnet for consecutive rain days."""
    pnet = pr_data.copy()
    n_time = len(pr_data)
    
    # Find consecutive rain day indices
    consec_inds = []
    for i in range(n_time):
        if cat_data[i] == 2:
            consec_inds.append(i)
    
    # Process consecutive rain events
    i = 0
    while i < len(consec_inds):
        # Find the end of current consecutive event
        event_start = i
        while i < len(consec_inds) - 1 and consec_inds[i+1] == consec_inds[i] + 1:
            i += 1
        event_end = i
        
        # Process this consecutive rain event
        accpr = 0.0
        thresh_flag = False
        
        for j in range(event_start, event_end + 1):
            ind = consec_inds[j]
            accpr += pr_data[ind]
            
            if accpr <= acc_thresh and not thresh_flag:
                pnet[ind] = 0.0
            elif accpr > acc_thresh and not thresh_flag:
                accpr -= acc_thresh
                pnet[ind] = accpr
                thresh_flag = True
            else:
                pnet[ind] = pr_data[ind]
        
        i += 1
    
    return pnet

@jit(nopython=True)
def _calc_kbdi_timeseries_numba(T_data, pnet_data, mean_ann_pr, day_int):
    """Numba-optimized KBDI time series calculation."""
    n_time = len(T_data)
    KBDI = np.full(n_time, np.nan)
    
    if day_int >= 0 and day_int < n_time:
        KBDI[day_int] = 0.0
        
        denominator = 1 + 10.88 * np.exp(-0.0441 * mean_ann_pr)
        
        for it in range(day_int + 1, n_time):
            Q = max(0.0, KBDI[it-1] - pnet_data[it] * 100)
            numerator = (800 - Q) * (0.968 * np.exp(0.0486 * T_data[it]) - 8.3)
            KBDI[it] = Q + (numerator / denominator) * 1e-3
    
    return KBDI

def calc_kbdi_vectorized(T, PR):
    """
    Vectorized KBDI calculation for multiple grid points.
    
    Parameters:
    -----------
    T : xarray.DataArray
        Temperature data with dimensions (time, lat, lon) in Fahrenheit
    PR : xarray.DataArray  
        Precipitation data with dimensions (time, lat, lon) in inches
    
    Returns:
    --------
    KBDI : xarray.DataArray
        KBDI values with same dimensions as input
    """
    
    # Create land mask - only calculate where we have finite precipitation
    landmask = np.isfinite(PR.mean('time'))
    
    # Create time index
    time_index = np.arange(len(PR.time)).astype('int')
    PR = PR.assign_coords(time_index=('time', time_index))
    T = T.assign_coords(time_index=('time', time_index))
    
    # Parameters
    ndays = 7
    pr_thresh = 8  # inches
    acc_thresh = 0.2  # inches
    
    # Calculate 7-day rolling precipitation sum
    pr_weeksum = PR.rolling(time=ndays, min_periods=ndays, center=False).sum()
    
    # Find saturation days (vectorized across space)
    def find_first_saturation_day(pr_week_1d):
        """Find first day where weekly precip exceeds threshold."""
        if np.isnan(pr_week_1d).all():
            return -1
        
        exceeds = pr_week_1d > pr_thresh
        if not exceeds.any():
            return -1
        
        return int(np.argmax(exceeds))
    
    # Apply across lat/lon dimensions
    saturation_days = xr.apply_ufunc(
        find_first_saturation_day,
        pr_weeksum,
        input_core_dims=[['time']],
        output_dtypes=[int],
        vectorize=True,
        dask='parallelized'
    )
    
    # Calculate consecutive rain days (vectorized)
    rainmask = xr.where(PR > 0, 1, 0).astype('int8')
    temp=rainmask.cumsum('time').astype('int8')
    rr = temp - temp.where(rainmask == 0).ffill(dim='time').fillna(0).astype('int8')

    # Calculate mean annual precipitation
    years = pd.to_datetime(PR.time.values).year
    PR_with_year = PR.assign_coords(year=('time', years))
    annual_sums = PR_with_year.groupby('year').sum(min_count=360)
    mean_ann_pr = annual_sums.mean('year')
    
    # Define function to process a single grid point
    def process_single_point(pr_1d, t_1d, rr_1d, mean_ann_pr_val, sat_day):
        """Process KBDI for a single lat/lon point."""
        if np.isnan(mean_ann_pr_val) or sat_day < 0:
            return np.full(len(pr_1d), np.nan) 

        # Calculate rainfall category 
        cat_1d = np.where(rr_1d >= 3, 5, rr_1d)
        # find the indexes of all second consecutive rainfall days
        consec_day2 = np.where(rr_1d == 2)
        consec_day2 = [arr.astype('int32') for arr in consec_day2]
        # find the indexes of all the first consecutive rainfall days
        consec_day1 = [arr-1 for arr in consec_day2]
        # label all consecutive rainfall days with a 2 
        cat_1d[consec_day2] = 5 # first put a 5
        cat_1d[consec_day1] = 5 # first put a 5
        cat_1d = np.where(cat_1d == 5, 2, cat_1d) # convert to 2's
    
        
        # Calculate Pnet for consecutive rain days
        pnet_1d = _calc_pnet_consecutive_numba(
            pr_1d.astype(np.float64), 
            cat_1d.astype(np.int32), 
            acc_thresh
        )
        
        # Apply single rain day adjustment
        single_mask = (cat_1d == 1)
        pnet_1d = np.where(single_mask, np.maximum(0, pnet_1d - acc_thresh), pnet_1d)
        
        # Calculate KBDI time series
        kbdi_1d = _calc_kbdi_timeseries_numba(
            t_1d.astype(np.float64),
            pnet_1d.astype(np.float64),
            float(mean_ann_pr_val),
            int(sat_day)
        )
        
        return kbdi_1d
    
    # Apply the function across all grid points
    KBDI = xr.apply_ufunc(
        process_single_point,
        PR.swap_dims({'time': 'time_index'}),
        T.swap_dims({'time': 'time_index'}),
        rr.swap_dims({'time': 'time_index'}),
        mean_ann_pr,
        saturation_days,
        input_core_dims=[['time_index'], ['time_index'], ['time_index'], [], []],
        output_core_dims=[['time_index']],
        output_dtypes=[float],
        vectorize=True,
        dask='parallelized'
    )
    
    # Convert back to original time coordinate
    KBDI = KBDI.swap_dims({'time_index': 'time'})
    KBDI = KBDI.assign_coords(time=PR.time)

    # return PNET
    return KBDI

def calc_kbdi_parallel_chunked(T, PR, chunks=None):
    """
    Parallel chunked version of KBDI calculation using Dask.
    
    Parameters:
    -----------
    T : xarray.DataArray
        Temperature data with dimensions (time, lat, lon) - will be converted to Fahrenheit
    PR : xarray.DataArray  
        Precipitation data with dimensions (time, lat, lon) - will be converted to inches
    chunks : dict, optional
        Chunk sizes for dask arrays, e.g., {'lat': 10, 'lon': 10, 'time': -1}
    
    Returns:
    --------
    KBDI : xarray.DataArray
        KBDI values with same dimensions as input
    """
    
    if chunks is None:
        # Default chunking strategy
        chunks = {'lat': min(20, T.sizes['lat']), 
                  'lon': min(20, T.sizes['lon']), 
                  'time': -1}  # Keep time together
    
    # Chunk the input data FIRST
    T_chunked = T.chunk(chunks)
    PR_chunked = PR.chunk(chunks)
    
    # THEN apply unit conversions (done in parallel by Dask)
    T_chunked = (T_chunked * 9/5) + 32  # Convert to Fahrenheit
    T_chunked.attrs = {'standard_name': 'air_temperature',
                       'long_name': 'Temperature, daily maximum',
                       'units': 'F'}
    
    PR_chunked = PR_chunked / 25.4  # Convert to inches
    PR_chunked.attrs = {'standard_name': 'precipitation', 
                        'long_name': 'Precipitation, daily total', 
                        'units': 'inches/day'}
    
    # Apply the vectorized calculation
    KBDI = calc_kbdi_vectorized(T_chunked, PR_chunked)
    # CAT = calc_kbdi_vectorized(T_chunked, PR_chunked)
    
    return KBDI


pr_file = r'D://data/nclimgrid_daily/prcp_nClimGridDaily_1951-2024_USsouth.nc'
tmax_file = r'D://data/nclimgrid_daily/tmax_nClimGridDaily_1951-2024_USsouth.nc'

year_start='1951'
year_end='2024'
lat1, lat2 = 32, 34
lon1, lon2 = -90, -88

# Load the full spatial domain you want to process:
pr = xr.open_dataset(pr_file).prcp.sel(time=slice(year_start,year_end),lat=slice(lat1,lat2), lon=slice(lon1,lon2))
tmax = xr.open_dataset(tmax_file).tmax.sel(time=slice(year_start,year_end),lat=slice(lat1,lat2), lon=slice(lon1,lon2)) 

# Then use the optimized function:
chunks = {'lat': 24, 'lon': 24, 'time': -1}  # Adjust based on your memory
kbdi_result = calc_kbdi_parallel_chunked(tmax, pr, chunks=chunks)
kbdi_result = kbdi_result.compute()  # Trigger computation