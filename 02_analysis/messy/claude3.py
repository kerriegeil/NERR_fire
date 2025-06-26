import xarray as xr
import numpy as np
import pandas as pd
from numba import jit
import dask
import dask.array as da
import matplotlib.pyplot as plt


@jit(nopython=True)
def _calc_pnet_consecutive_numba(pr_data, cat_data, acc_thresh=0.2):
    """Numba-optimized function to calculate Pnet for consecutive rain days."""
    pnet = pr_data.copy()
    n_time = len(pr_data)
    
    # Find consecutive rain day indices - more efficient approach
    i = 0
    while i < n_time:
        if cat_data[i] == 2:
            # Found start of consecutive rain event
            event_start = i
            # Find end of consecutive event
            while i < n_time and cat_data[i] == 2:
                i += 1
            event_end = i - 1
            
            # Process this consecutive rain event
            accpr = np.float32(0)
            thresh_flag = False
            
            for j in range(event_start, event_end + 1):
                accpr += pr_data[j]
                
                if accpr <= acc_thresh and not thresh_flag:
                    pnet[j] = np.float32(0)
                elif accpr > acc_thresh and not thresh_flag:
                    accpr -= acc_thresh
                    pnet[j] = accpr
                    thresh_flag = True
                else:
                    pnet[j] = pr_data[j]
        else:
            i += 1
    
    return pnet


@jit(nopython=True)
def _calc_kbdi_timeseries_numba(T_data, pnet_data, mean_ann_pr, day_int):
    """Numba-optimized KBDI time series calculation."""
    n_time = len(T_data)
    KBDI = np.full(n_time, np.float32(np.nan))
    
    if day_int >= 0 and day_int < n_time:
        KBDI[day_int] = np.float32(0)
        
        # Pre-calculate constant denominator
        denominator = np.float32(1 + 10.88 * np.exp(-0.0441 * mean_ann_pr))
        inv_denominator = np.float32(1e-3) / denominator  # Pre-calculate division
        
        for it in range(day_int + 1, n_time):
            Q = max(np.float32(0), KBDI[it-1] - pnet_data[it] *np.float32(100))
            numerator = np.float32((800 - Q) * (0.968 * np.exp(0.0486 * T_data[it]) - 8.3))
            KBDI[it] = Q + numerator * inv_denominator
    
    return KBDI


@jit(nopython=True)
def _calculate_consecutive_rain_categories_numba(rainmask):
    """Optimized calculation of consecutive rain day categories."""
    n_time = len(rainmask)
    cat_data = np.zeros(n_time, dtype=np.int8)
    
    i = 0
    while i < n_time:
        if rainmask[i] > 0:
            # Found start of rain event
            event_start = i
            event_length = 0
            
            # Count consecutive rain days
            while i < n_time and rainmask[i] > 0:
                event_length += 1
                i += 1
            
            # Assign categories based on event length
            if event_length == 1:
                cat_data[event_start] = 1  # Single rain day
            else:
                # Multiple consecutive rain days
                for j in range(event_start, event_start + event_length):
                    cat_data[j] = 2
        else:
            i += 1
    
    return cat_data


def calc_kbdi_vectorized_optimized(T, PR):
    """
    Optimized vectorized KBDI calculation for multiple grid points.
    
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
    # Create time index
    time_index = np.arange(len(PR.time), dtype=np.int32)
    PR = PR.assign_coords(time_index=('time', time_index))
    T = T.assign_coords(time_index=('time', time_index))
    
    # Parameters
    ndays = np.int8(7)
    pr_thresh = np.float32(8.0)  # inches
    acc_thresh = np.float32(0.2)  # inches
    

    # LAZY CALCULATIONS ON CHUNKED XARRAY (DASK) ARRAYS

    # Calculate 7-day rolling precipitation sum - more efficient
    pr_weeksum = PR.rolling(time=ndays, min_periods=ndays, center=False).sum('time')

    # Calculate mean annual precipitation
    mean_ann_pr = PR.groupby('time.year').sum(min_count=360).mean('year')
    
    # GRID-BY-GRID CALCULATIONS
    # VECTORIZED AND PARALLELIZED WITH XARRAY APPLY_UFUNC AND DASK
    # OPTIMIZED WITH NUMBA
    
    # Optimized saturation day finding
    def find_first_saturation_day_optimized(pr_week_1d):
        """Optimized version using numpy operations."""
        valid_mask = ~np.isnan(pr_week_1d)
        if not valid_mask.any():
            return -1
        
        exceeds = pr_week_1d > pr_thresh
        if not exceeds.any():
            return -1
        
        return int(np.argmax(exceeds))
    
    # Apply across lat/lon dimensions
    saturation_days = xr.apply_ufunc(
        find_first_saturation_day_optimized,
        pr_weeksum,
        input_core_dims=[['time']],
        output_dtypes=[np.int32],
        vectorize=True,
        dask='parallelized')

    # Define optimized function to process a single grid point
    def process_single_point_optimized(pr_1d, t_1d, mean_ann_pr_val, sat_day):
        """Optimized processing for a single lat/lon point."""
        if np.isnan(mean_ann_pr_val) or sat_day < 0:
            return np.full(len(pr_1d), np.float32(np.nan))

        # Create rain mask (0 or 1) - more efficient
        rainmask = (pr_1d > 0).astype(np.int8)
        
        # Calculate rainfall categories using optimized numba function
        cat_1d = _calculate_consecutive_rain_categories_numba(rainmask)
        
        # Calculate Pnet for consecutive rain days
        pnet_1d = _calc_pnet_consecutive_numba(
            pr_1d, 
            cat_1d, 
            acc_thresh)
        
        # Apply single rain day adjustment - vectorized
        single_mask = (cat_1d == 1)
        pnet_1d = np.where(single_mask, np.maximum(np.float32(0), pnet_1d - acc_thresh), pnet_1d)
        
        # Calculate KBDI time series
        kbdi_1d = _calc_kbdi_timeseries_numba(
            t_1d,
            pnet_1d,
            mean_ann_pr_val,
            sat_day)
        
        return kbdi_1d
    
    # Apply the function across all grid points
    KBDI = xr.apply_ufunc(
        process_single_point_optimized,
        PR.swap_dims({'time': 'time_index'}),
        T.swap_dims({'time': 'time_index'}),
        mean_ann_pr,
        saturation_days,
        input_core_dims=[['time_index'], ['time_index'], [], []],
        output_core_dims=[['time_index']],
        output_dtypes=[np.float32],
        vectorize=True,
        dask='parallelized'
    )
    
    # Convert back to original time coordinate
    KBDI = KBDI.swap_dims({'time_index': 'time'})
    KBDI = KBDI.assign_coords(time=PR.time)

    return KBDI


def calc_kbdi_parallel_chunked_optimized(T, PR, chunks=None):
    """
    Optimized parallel chunked version of KBDI calculation using Dask.
    
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
        # Optimized default chunking strategy
        chunks = {'lat': min(32, T.sizes['lat']), 
                  'lon': min(32, T.sizes['lon']), 
                  'time': -1}  # Keep time together
    
    # Chunk the input data FIRST
    T_chunked = T.chunk(chunks)
    PR_chunked = PR.chunk(chunks)
    
    # Apply precision limit and unit conversions in parallel
    T_chunked = T_chunked.round(2).astype('float32')  # round to 2 decimal places
    T_chunked = T_chunked * np.float32(9/5) + np.float32(32.0)  # Convert to Fahrenheit 
    T_chunked.attrs = {'standard_name': 'air_temperature',
                       'long_name': 'Temperature, daily maximum',
                       'units': 'F'}

    PR_chunked = PR_chunked.round(2).astype('float32')
    PR_chunked = PR_chunked * np.float32(1/25.4)  # Convert to inches - optimized
    PR_chunked.attrs = {'standard_name': 'precipitation', 
                        'long_name': 'Precipitation, daily total', 
                        'units': 'inches/day'}
    
    # Apply the optimized vectorized calculation
    KBDI = calc_kbdi_vectorized_optimized(T_chunked, PR_chunked)
    
    return KBDI


pr_file = r'D://data/nclimgrid_daily/prcp_nClimGridDaily_1951-2024_USsouth.nc'
tmax_file = r'D://data/nclimgrid_daily/tmax_nClimGridDaily_1951-2024_USsouth.nc'

year_start = '1951'
year_end = '2024'
lat1, lat2 = 32, 34
lon1, lon2 = -90, -88

# Load data with optimized settings
with xr.open_dataset(pr_file) as ds_pr:
    pr = ds_pr.prcp.sel(time=slice(year_start, year_end), 
                       lat=slice(lat1, lat2), 
                       lon=slice(lon1, lon2))

with xr.open_dataset(tmax_file) as ds_tmax:
    tmax = ds_tmax.tmax.sel(time=slice(year_start, year_end), 
                           lat=slice(lat1, lat2), 
                           lon=slice(lon1, lon2))

# Optimized chunking strategy
chunks = {'lat': 32, 'lon': 32, 'time': -1}  # Larger chunks for better performance

# Run optimized calculation
kbdi_result = calc_kbdi_parallel_chunked_optimized(tmax, pr, chunks=chunks)
kbdi_result = kbdi_result.compute()  # Trigger computation