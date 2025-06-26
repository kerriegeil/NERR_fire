import xarray as xr
import numpy as np
import pandas as pd
from numba import jit
import dask
import dask.array as da
from dask import delayed
import matplotlib.pyplot as plt
import gc

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


@jit(nopython=True,fastmath=True)
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


# @delayed
def process_chunk_delayed(pr_np, t_np, mean_ann_pr_np, saturation_days_np):
    """
    Delayed function to process a single spatial chunk.
    """

    # Get dimensions
    n_time, n_lat, n_lon = pr_np.shape
    
    # Initialize output array
    kbdi_chunk = np.full((n_time, n_lat, n_lon), np.float32(np.nan))
    
    # Process each grid point in this chunk
    for i in range(n_lat):
        for j in range(n_lon):
            pr_1d = pr_np[:, i, j]
            t_1d = t_np[:, i, j]
            mean_ann_pr_val = mean_ann_pr_np[i, j]
            sat_day = sat_days_np[i, j]
            
            # Skip if invalid data
            if np.isnan(mean_ann_pr_val) or sat_day < 0:
                continue
                
            # Parameters
            acc_thresh = np.float32(0.2)
            
            # Create rain mask
            rainmask = (pr_1d > 0).astype(np.int8)
            
            # Calculate rainfall categories
            cat_1d = _calculate_consecutive_rain_categories_numba(rainmask)
            
            # Calculate Pnet for consecutive rain days
            pnet_1d = _calc_pnet_consecutive_numba(pr_1d, cat_1d, acc_thresh)
            
            # Apply single rain day adjustment
            single_mask = (cat_1d == 1)
            pnet_1d = np.where(single_mask, np.maximum(np.float32(0), pnet_1d - acc_thresh), pnet_1d)
            
            # Calculate KBDI time series
            kbdi_1d = _calc_kbdi_timeseries_numba(t_1d, pnet_1d, mean_ann_pr_val, sat_day)
            
            # Store result
            kbdi_chunk[:, i, j] = kbdi_1d
    
    return kbdi_chunk


def calc_kbdi_dask_delayed(T, PR, spatial_chunk_size=(20, 20)):
    """
    Calculate KBDI using dask delayed for efficient parallel processing.
    
    Parameters:
    -----------
    T : xarray.DataArray
        Temperature data with dimensions (time, lat, lon) in Fahrenheit
    PR : xarray.DataArray  
        Precipitation data with dimensions (time, lat, lon) in inches
    spatial_chunk_size : tuple
        Chunk sizes for lat and lon dimensions
    
    Returns:
    --------
    KBDI : xarray.DataArray
        KBDI values with same dimensions as input
    """
    coords = PR.coords.copy()
    
    # Parameters
    ndays = np.int8(7)
    pr_thresh = np.float32(8.0)  # inches
    
    # Calculate 7-day rolling precipitation sum
    print("Calculating 7-day precipitation sums...")
    pr_weeksum = PR.rolling(time=ndays, min_periods=ndays, center=False).sum('time')
    
    # Calculate mean annual precipitation
    print("Calculating mean annual precipitation...")
    mean_ann_pr = PR.groupby('time.year').sum(min_count=360).mean('year')
    
    # Find saturation days
    print("Finding saturation days...")
    def find_first_saturation_day_optimized(pr_week_1d):
        """Optimized version using numpy operations."""
        valid_mask = ~np.isnan(pr_week_1d)
        if not valid_mask.any():
            return -1
        
        exceeds = pr_week_1d > pr_thresh
        if not exceeds.any():
            return -1
        
        return int(np.argmax(exceeds))
    
    saturation_days = xr.apply_ufunc(
        find_first_saturation_day_optimized,
        pr_weeksum,
        input_core_dims=[['time']],
        output_dtypes=[np.int32],
        vectorize=True,
        dask='parallelized')
    
    mean_ann_pr_chunked = mean_ann_pr.chunk({'lat': spatial_chunk_size[0], 'lon': spatial_chunk_size[1]})
    saturation_days_chunked = saturation_days.chunk({'lat': spatial_chunk_size[0], 'lon': spatial_chunk_size[1]})
    
    # Convert to dask delayed objects
    print("Converting to delayed objects...")
    pr_delayed = PR.data.to_delayed().ravel()
    t_delayed = T.data.to_delayed().ravel()
    mean_ann_pr_delayed = mean_ann_pr_chunked.data.to_delayed().ravel()
    sat_days_delayed = saturation_days_chunked.data.to_delayed().ravel()
    print(len(pr_delayed),len(t_delayed),len(mean_ann_pr_delayed),len(sat_days_delayed))
    
    # Create delayed computation tasks
    print(f"Creating {len(pr_delayed)} delayed computation tasks...")
    zipvars=zip(pr_delayed, t_delayed, mean_ann_pr_delayed, sat_days_delayed)
    task_list=[dask.delayed(process_chunk_delayed)(pr_chunk, t_chunk, mean_pr_chunk, sat_chunk) for pr_chunk, t_chunk, mean_pr_chunk, sat_chunk in zipvars] # list of compute tasks
    
    # Compute all chunks in parallel
    print("Computing all chunks in parallel...")
    kbdi_chunks = dask.compute(*task_list)
    print(f'nchunks = {len(kbdi_chunks)}, chunk shape = {kbdi_chunks[0].shape}')
    
    print("Reconstructing full array...")
    kbdi_np = np.concatenate(kbdi_chunks,axis=1)
    KBDI = xr.DataArray(kbdi_np,name='kbdi',coords=coords)
    
    return KBDI


def calc_kbdi_dask_optimized(pr_file, tmax_file, year_start='1951', year_end='2024', 
                            spatial_chunk_size=(20, 20), memory_limit='20GB'):
    """
    Memory-efficient KBDI calculation using dask delayed.
    
    Parameters:
    -----------
    pr_file : str
        Path to precipitation NetCDF file
    tmax_file : str  
        Path to temperature NetCDF file
    year_start : str
        Start year for processing
    year_end : str
        End year for processing
    spatial_chunk_size : tuple
        Chunk sizes for (lat, lon) dimensions
    memory_limit : str
        Memory limit for dask workers
    
    Returns:
    --------
    KBDI : xarray.DataArray
        KBDI values
    """
    
    # Configure Dask for optimal performance with your RAM
    dask.config.set({
        'array.chunk-size': '100MB',
        'array.slicing.split_large_chunks': True,
        'distributed.worker.memory.target': 0.75,  # Use 75% of available memory
        'distributed.worker.memory.spill': 0.85,   # Spill to disk at 85%
        'distributed.worker.memory.pause': 0.95,   # Pause at 95%
        'distributed.worker.memory.terminate': 0.98  # Terminate at 98%
    })
    
    # Load data with chunking - keep time intact but chunk spatially
    load_chunks = {
        'time': -1,  # Keep entire time series
        'lat': spatial_chunk_size[0], 
        'lon': spatial_chunk_size[1]
    }
    
    print(f"Loading precipitation data with chunks: {load_chunks}")
    pr = xr.open_dataset(pr_file, chunks=load_chunks).prcp.sel(time=slice(year_start, year_end))

        
    print(f"Loading temperature data with chunks: {load_chunks}")
    tmax = xr.open_dataset(tmax_file, chunks=load_chunks).tmax.sel(time=slice(year_start, year_end))

    # Convert units efficiently
    print("Converting units...")
    tmax = tmax * np.float32(9/5) + np.float32(32.0)  # Convert to Fahrenheit
    tmax.attrs = {'standard_name': 'air_temperature',
                  'long_name': 'Temperature, daily maximum',
                  'units': 'F'}

    pr = pr * np.float32(1/25.4)  # Convert to inches
    pr.attrs = {'standard_name': 'precipitation', 
                'long_name': 'Precipitation, daily total', 
                'units': 'inches/day'}
    
    # Calculate KBDI using dask delayed
    kbdi_result = calc_kbdi_dask_delayed(tmax, pr, spatial_chunk_size)
    
    # Add metadata
    kbdi_result.attrs = {
        'standard_name': 'keetch_byram_drought_index',
        'long_name': 'Keetch-Byram Drought Index',
        'units': 'dimensionless'
    }
    
    return kbdi_result

# Main execution
if __name__ == "__main__":
    pr_file = r'D://data/nclimgrid_daily/prcp_nClimGridDaily_1951-2024_USsouth.nc'
    tmax_file = r'D://data/nclimgrid_daily/tmax_nClimGridDaily_1951-2024_USsouth.nc'
    
    year_start = '1951'
    year_end = '2024'
    
    # Try different chunk sizes - start with larger chunks for efficiency
    chunk_sizes_to_try = [(-1, 11)]
    
    for chunk_size in chunk_sizes_to_try:
        try:
            print(f"\n{'='*60}")
            print(f"Attempting with spatial chunk size: {chunk_size}")
            print(f"{'='*60}")
            
            kbdi_result = calc_kbdi_dask_optimized(
                pr_file, tmax_file, year_start, year_end, 
                spatial_chunk_size=chunk_size
            )
            
            print("\nCalculation successful! Saving results...")
            output_file = f'kbdi_nclimgrid_{year_start}_{year_end}_dask.nc'

            
            kbdi_result.to_netcdf(output_file)#, encoding=encoding)
            print(f"Results saved to {output_file}")
            print(f"Final data shape: {kbdi_result.shape}")
            print(f"Success with chunk size: {chunk_size}")
            break
            
        except MemoryError as e:
            print(f"Memory error with chunk size {chunk_size}: {e}")
            if chunk_size == chunk_sizes_to_try[-1]:
                print("ERROR: All chunk sizes failed!")
                raise
            else:
                print("Trying smaller chunk size...")
                gc.collect()
                continue
                
        except Exception as e:
            print(f"Unexpected error with chunk size {chunk_size}: {e}")
            if chunk_size == chunk_sizes_to_try[-1]:
                raise
            else:
                print("Trying smaller chunk size...")
                gc.collect()
                continue