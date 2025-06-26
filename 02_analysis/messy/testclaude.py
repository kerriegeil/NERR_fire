import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import dask

h_file = 'pr_Amon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_185001-201412.nc'
f_file = 'pr_Amon_ACCESS-ESM1-5_ssp245_r1i1p1f1_gn_201501-210012.nc'

ds = xr.open_mfdataset([h_file,f_file]).sel(lat=39.29,lon=283.39, method='nearest')
pr_ann = ds.pr.groupby('time.year').mean('time').compute()
pr_ann = pr_ann*86400.
pr_ann.attrs['units']='mm/day'
pr_ann.plot(figsize=(10,2))