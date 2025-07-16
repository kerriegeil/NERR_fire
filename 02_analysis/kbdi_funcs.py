#!/usr/bin/env python
"""
Author: K Geil
Date: Jul 2025
Description: Functions to calculate Keetch-Byram Drough Index
"""

import xarray as xr
import numpy as np
import pandas as pd

# lazy read data and convert units, return xarray object backed with chunked dask arrays
# can process subsets in all dimensions or subset only in time, but not subset in either lat or lon, need both or none

def get_pr(pr_file,chunks,year_start,year_end,lat1=None,lat2=None,lon1=None,lon2=None):
    # lazy read data and chunk it
    if lat1 is not None:
        pr = xr.open_dataset(pr_file).prcp.sel(time=slice(year_start, year_end),lat=slice(lat1,lat2), lon=slice(lon1,lon2)).chunk(chunks)
    else:
        pr = xr.open_dataset(pr_file).prcp.sel(time=slice(year_start, year_end)).chunk(chunks)
    
    # convert units
    pr = (pr / 25.4).round(2).astype('float32')  # convert to inches
    return pr
  

def get_tmax(tmax_file,chunks,year_start,year_end,lat1=None,lat2=None,lon1=None,lon2=None):
    # lazy read data and chunk it
    if lat1 is not None:
        tmax = xr.open_dataset(tmax_file).tmax.sel(time=slice(year_start, year_end),lat=slice(lat1,lat2), lon=slice(lon1,lon2)).chunk(chunks)
    else:
        tmax = xr.open_dataset(tmax_file).tmax.sel(time=slice(year_start, year_end)).chunk(chunks)

    # convert units
    tmax = ((tmax * 9 / 5) + 32).round(2).astype('float32')  # convert to Fahrenheit
    return tmax

def get_chunk_info(var):
    # get chunk coordinate information
    
    # dim sizes for each chunk
    time_chunks, lat_chunks, lon_chunks = var.chunks
    # it,iy,ix
    
    # chunk bounds
    time_chunkbounds = [sum(time_chunks[:i]) for i in range(len(time_chunks)+1)]
    lat_chunkbounds = [sum(lat_chunks[:i]) for i in range(len(lat_chunks)+1)]
    lon_chunkbounds = [sum(lon_chunks[:i]) for i in range(len(lon_chunks)+1)]
    
    # build a list of coordinates for each result chunk
    coord_list=[]
    for it,tind in enumerate(time_chunkbounds[:-1]):
        for iy,yind in enumerate(lat_chunkbounds[:-1]):
            for ix,xind in enumerate(lon_chunkbounds[:-1]):
                coords = var.isel(time=slice(tind,time_chunkbounds[it+1]),
                                 lat=slice(yind,lat_chunkbounds[iy+1]),
                                 lon=slice(xind,lon_chunkbounds[ix+1])).coords
                coord_list.append(coords)
    return coord_list

# functions operate on xarray data arrays

def calc_mean_pr(pr):
    # mean annual precipitation at each grid
    mean_ann_pr = pr.groupby('time.year').sum(min_count=360).mean('year')
    return mean_ann_pr

def find_initialization_index(pr):
    # create a land mask
    # landmask = xr.where(np.isfinite(mean_ann_pr),1,0).astype('int32')

    # find the saturation/initialization date (8 inches precip over a week)
    
    # constants
    ndays=7
    thresh=8  # inches
    badval=-100000  # value to use for grids that never reach saturation
    
    # rolling weekly sum of precipitation
    pr_weeksum=pr.rolling(time=ndays,min_periods=ndays,center=False).sum('time')   
    
    # quantify how many grids are never saturated
    threshmask = xr.where((pr_weeksum>=thresh).sum('time')>0,1,0) # 1=init date found, 0=no init date found
    # nbad = xr.where((landmask)&(threshmask==0),1,0).sum().item() # 1=grid on land with data but no init date found

    # save the initialization index for each grid
    day_int = (pr_weeksum>=thresh).argmax('time').astype('int32') # will yield 0 if no saturation reached
    day_int = xr.where(threshmask,day_int,badval).astype('int32') # indicate grids that don't reach saturation with badval

    return day_int

def consec_rainday_int(pr):
    rainmask=xr.where(pr>0,1,0) # 1/0 rain/no rain mask
    rr = rainmask.cumsum('time')-rainmask.cumsum('time').where(rainmask == 0).ffill(dim='time').fillna(0)    
    return rr.astype('float32')

# functions that operate on numpy arrays

def rain_categories(rr):
    # # Rain mask and consecutive rain days
    # rainmask = np.where(pr > 0, 1, 0).astype('int32')
    # cumsum = np.cumsum(rainmask,axis=0)
    # reset = np.where(rainmask == 0, cumsum, np.nan)
    # reset = np.maximum.accumulate(np.where(np.isnan(reset), -1, reset),axis=0)
    # # rr_np = cumsum - reset
    # rr = cumsum - reset

    # Categorize rain days
    cat = np.where(rr >= 3, 5, rr)
    consecday2_timeind,consecday2_latind,consecday2_lonind = np.where(rr == 2)
    consecday1_timeind = consecday2_timeind - 1
    cat[consecday2_timeind,consecday2_latind,consecday2_lonind] = 5
    cat[consecday1_timeind,consecday2_latind,consecday2_lonind]  = 5
    cat = np.where(cat == 5, 2, cat)
    return cat

def calc_pnet_vectorized(PR, cat, acc_thresh=0.2):
    """
    Vectorized calculation of pnet for 3D arrays
    
    Parameters:
    PR: 3D array (time, lat, lon) - precipitation data
    cat: 3D array (time, lat, lon) - category data (0, 1, or 2)
    acc_thresh: float - accumulation threshold
    
    Returns:
    pnet: 3D array (time, lat, lon) - processed precipitation
    """
    time_steps, lat_size, lon_size = PR.shape
    pnet = PR.copy()
    
    # Handle category 1 (single rain days) - vectorized
    cat1_mask = (cat == 1)
    pnet = np.where(cat1_mask, pnet - acc_thresh, pnet)
    pnet = np.where(pnet < 0, 0, pnet)
    del cat1_mask
    
    # Handle category 2 (consecutive rain days) - needs loop over time
    accpr = np.zeros((lat_size, lon_size))  # 2D accumulation array
    thresh_flag = np.zeros((lat_size, lon_size), dtype=bool)  # 2D flag array
    
    for t in range(time_steps):
        cat2_mask = (cat[t] == 2)  # 2D mask for current time step
        
        # Only process locations with category 2 on this day
        if np.any(cat2_mask):
            # Accumulate precipitation
            accpr[cat2_mask] += PR[t, cat2_mask]
            
            # Check if we need to reset for new consecutive events
            # This requires detecting breaks in consecutive sequences
            if t > 0:
                prev_cat2 = (cat[t-1] == 2)
                new_event_mask = cat2_mask & ~prev_cat2
                accpr[new_event_mask] = PR[t, new_event_mask]
                thresh_flag[new_event_mask] = False
            
            # Apply threshold logic
            # Case 1: Below threshold and flag not set
            below_thresh_mask = cat2_mask & (accpr <= acc_thresh) & ~thresh_flag
            pnet[t, below_thresh_mask] = 0
            
            # Case 2: Above threshold and flag not set (first time over threshold)
            above_thresh_mask = cat2_mask & (accpr > acc_thresh) & ~thresh_flag
            pnet[t, above_thresh_mask] = accpr[above_thresh_mask] - acc_thresh
            thresh_flag[above_thresh_mask] = True
            
            # Case 3: Flag already set (subsequent days after threshold met)
            # pnet remains unchanged (already copied from PR)
            
            # Reset accumulation for locations where consecutive event ends
            if t < time_steps - 1:
                next_cat2 = (cat[t+1] == 2) if t+1 < time_steps else np.zeros_like(cat2_mask)
                end_event_mask = cat2_mask & ~next_cat2
                accpr[end_event_mask] = 0
                thresh_flag[end_event_mask] = False
    
    return pnet.round(3)

def write_chunk_nc(ID,arr,coords):
    var_attrs={'standard_name':'kbdi',
           'long_name':'Keetch_Byram_Drought_Index',
           'units':'hundredths of an inch',
           'description': 'represents the departure from saturation to a depth of 8 inches',
           'valid_min':0,
           'valid_max':800,
           'comment':'from process_kbdi_AllDist.ipynb'}
    ID = str(ID).zfill(3)
    kbdi = xr.DataArray(arr,coords=coords,name='kbdi')
    kbdi.attrs=var_attrs
    kbdi.to_netcdf(f'output/kbdi_chunk_{ID}.nc')

def calc_kbdi(ID,pr,tmax,coords):
    # pr = (pr / 25.4).round(2).astype('float32')  # convert to inches
    # tmax = ((tmax * 9 / 5) + 32).round(2).astype('float32')  # convert to Fahrenheit    
    
    # xarray object for xarray funcs
    pr_xr = xr.DataArray(pr, coords=coords, name='pr')
    mean_ann_pr_xr = calc_mean_pr(pr_xr)
    day_int_xr = find_initialization_index(pr_xr)
    rr_xr = consec_rainday_int(pr_xr)

    # Convert back to numpy for rest of calculation
    mean_ann_pr = mean_ann_pr_xr.data
    day_int = day_int_xr.data
    rr = rr_xr.data  
    del pr_xr,mean_ann_pr_xr,day_int_xr,rr_xr

    cat = rain_categories(rr)
    pnet = calc_pnet_vectorized(pr, cat) 
    del cat
    
    # KBDI calculation (inches, Fahrenheit)
    # nan initialization
    KBDI = np.full(pr.shape,np.nan).astype('float32')
    
    # Replace nan with 0 at init date for each grid
    ntimes,nlats,nlons=pr.shape
    lat_inds, lon_inds = np.meshgrid(np.arange(nlats), np.arange(nlons), indexing='ij')
    badval_mask = day_int<0  # mask for locations that never get saturated
    day_int[badval_mask]=0 # temporarily change never-saturated locations to 0 so calc doesn't error
    KBDI[day_int, lat_inds, lon_inds] = 0
    KBDI[:,badval_mask]=np.nan # replace never-saturated locations nan
    del badval_mask,day_int
    
    # time independent part of the equation
    denominator = 1 + 10.88 * np.exp(-0.0441*mean_ann_pr)
    
    # looping in time
    for it in range(ntimes):
        if it>0:
            # 2D flags to identify initialization date at each grid
            flag_today = np.isfinite(KBDI[it,:,:])
            flag_prev = np.isfinite(KBDI[it-1,:,:])
    
            # parts of the KBDI equation
            Q = KBDI[it-1,:,:] - pnet[it,:,:]*100
            Q = np.where(Q<0,0,Q) # correct any negatives
            numerator = (800 - Q) * (0.968 * np.exp(0.0486*tmax[it,:,:]) - 8.3)
            
            # replace nan KBDI with finite value only after initialization date at each grid
            # this happens when today's flag is False but yesterday's was True (finds the 0 at each grid)
            KBDI[it,:,:] = np.where((~flag_today)&(flag_prev),Q + (numerator/denominator)*1E-3,KBDI[it,:,:])  
            KBDI[it,:,:] = np.where(KBDI[it,:,:]< 0.0,0,KBDI[it,:,:])  # correct any negatives
    
            del Q,numerator

    write_chunk_nc(ID,KBDI,coords)
    return ID

