#!/usr/bin/env python
"""
Dask Configuration Module for HPC and Single Machine Processing
Author: K Geil
Date: Jul 2025
Description: Automatic configuration of Dask workers and chunk sizes based on system resources
"""

import xarray as xr
import numpy as np
import psutil
import os
import platform
import dask


def detect_system_type():
    """
    Detect the type of system we're running on
    
    Returns:
    --------
    str: 'hpc' or 'single_machine'
    """
    # Check for common HPC indicators
    hpc_indicators = [
        'SLURM_JOB_ID' in os.environ,
        'PBS_JOBID' in os.environ,
        'LSB_JOBID' in os.environ,
        'TORQUE_JOBID' in os.environ,
        'login' in platform.node().lower(),
        'compute' in platform.node().lower(),
        'hpc' in platform.node().lower(),
        'cluster' in platform.node().lower()
    ]
    
    if any(hpc_indicators):
        return 'hpc'
    else:
        return 'single_machine'


def get_system_recommendations(system_type, cpu_count, total_memory_gb):
    """
    Get system-specific recommendations
    
    Parameters:
    -----------
    system_type : str
        Type of system ('hpc' or 'single_machine')
    cpu_count : int
        Number of CPU cores
    total_memory_gb : float
        Total system memory in GB
    
    Returns:
    --------
    dict: System-specific recommendations
    """
    if system_type == 'hpc':
        return {
            'suggested_workers': min(cpu_count, 80),  # Cap at reasonable limit
            'memory_strategy': 'automatic',
        }
    else:  # single_machine
        return {
            'suggested_workers': max(1, int(cpu_count * 0.9)),
            'memory_strategy': 'balanced',
        }


def prompt_user_preferences(system_type, cpu_count, recommendations):
    """
    Enhanced interactive prompts for user preferences
    """
    print(f"\n{'='*60}")
    print(f"SYSTEM CONFIGURATION")
    print(f"{'='*60}")
    print(f"Detected system type: {system_type.upper()}")
    print(f"Available CPU cores: {cpu_count}")
    print(f"Recommended workers: {recommendations['suggested_workers']}")
    # print(f"Strategy: {recommendations['memory_strategy']}")
    print(f"{'='*60}\n")
    
    # Ask if user wants to override system detection
    override_system = input("Override system type detection? (y/n, default=n): ").lower().strip()
    if override_system in ['y', 'yes']:
        print("\nSystem types:")
        print("1. HPC (High Performance Computing)")
        print("2. Single Machine (Desktop/Laptop/Workstation)")
        
        while True:
            choice = input("Select system type (1-2): ").strip()
            if choice == '1':
                system_type = 'hpc'
                break
            elif choice == '2':
                system_type = 'single_machine'
                break
            else:
                print("Invalid choice. Please enter 1 or 2.")
        
        recommendations = get_system_recommendations(system_type, cpu_count, 
                                                   psutil.virtual_memory().total / (1024**3))
    
    # Ask about number of workers
    use_recommended = input(f"Use recommended {recommendations['suggested_workers']} workers? (y/n, default=y): ").lower().strip()
    
    if use_recommended in ['n', 'no']:
        while True:
            try:
                custom_workers = input(f"Enter number of workers (1-{cpu_count}): ").strip()
                custom_workers = int(custom_workers)
                if 1 <= custom_workers <= cpu_count:
                    nworkers = custom_workers
                    break
                else:
                    print(f"Please enter a number between 1 and {cpu_count}")
            except ValueError:
                print("Please enter a valid number")
    else:
        nworkers = recommendations['suggested_workers']
    
    # # Ask about chunks-per-worker target
    # default_target = 3.0
    # target_chunks_input = input(f"Target chunks per worker (default={default_target})").strip()
    # try:
    #     target_chunks_per_worker = float(target_chunks_input) if target_chunks_input else default_target
    #     target_chunks_per_worker = max(2.0, min(target_chunks_per_worker, 2.95))  # Cap at just under 3
    # except ValueError:
    #     print("Invalid input. Using default target.")
    #     target_chunks_per_worker = default_target

    # Ask about chunks-per-worker target
    default_target = 3.0
    target_chunks_input = input(f"Target chunks per worker (default={default_target}): ").strip()
    try:
        target_chunks_per_worker = float(target_chunks_input) if target_chunks_input else default_target
        target_chunks_per_worker = max(2.0, target_chunks_per_worker)  # Minimum of 2.0, no upper limit
    except ValueError:
        print("Invalid input. Using default target.")
        target_chunks_per_worker = default_target    
    
    # Ask about automatic chunk sizing
    auto_chunks = input("Try automatic chunk sizing? (y/n, default=y): ").lower().strip()
    use_auto_chunks = auto_chunks not in ['n', 'no']
    
    manual_chunks = None
    if not use_auto_chunks:
        print("\nEnter chunk sizes (press Enter for default values):")
        try:
            lat_chunk = input("Latitude chunk size (default=16): ").strip()
            lat_chunk = int(lat_chunk) if lat_chunk else 16
            
            lon_chunk = input("Longitude chunk size (default=16): ").strip()
            lon_chunk = int(lon_chunk) if lon_chunk else 16
            
            manual_chunks = {'time': -1, 'lat': lat_chunk, 'lon': lon_chunk}
        except ValueError:
            print("Invalid input. Using automatic chunk sizing.")
            use_auto_chunks = True
    
    return {
        'system_type': system_type,
        'nworkers': nworkers,
        'use_auto_chunks': use_auto_chunks,
        'manual_chunks': manual_chunks,
        'target_chunks_per_worker': target_chunks_per_worker,
        'recommendations': recommendations
    }


def auto_configure_processing(pr_file, tmax_file, year_start, year_end, 
                            interactive=True,
                            target_chunks_per_worker=3.0):
    """
    Automatically configure number of workers and chunk sizes based on system resources
    
    Parameters:
    -----------
    pr_file : str
        Path to precipitation file
    tmax_file : str  
        Path to temperature file
    year_start : str
        Start year for processing
    year_end : str
        End year for processing
    interactive : bool
        Whether to prompt user for preferences (default=True)
    target_chunks_per_worker : float
        Target ratio of chunks to workers for optimal efficiency (default=2.8)
    
    Returns:
    --------
    dict with keys: 'nworkers', 'chunks', 'memory_per_worker', 'system_info'
    """
    
    # Get system information
    system_type = detect_system_type()
    cpu_count = os.cpu_count()
    total_memory_gb = psutil.virtual_memory().total / (1024**3)
    available_memory_gb = psutil.virtual_memory().available / (1024**3)
    target_mem_percent = 0.75
    
    # Get system-specific recommendations
    recommendations = get_system_recommendations(system_type, cpu_count, total_memory_gb)
    
    # Get user preferences if interactive
    if interactive:
        user_prefs = prompt_user_preferences(system_type, cpu_count, recommendations)
        nworkers = user_prefs['nworkers']
        use_auto_chunks = user_prefs['use_auto_chunks']
        manual_chunks = user_prefs['manual_chunks']
        final_system_type = user_prefs['system_type']
        final_recommendations = user_prefs['recommendations']
        target_chunks_per_worker = user_prefs['target_chunks_per_worker']
    else:
        nworkers = recommendations['suggested_workers']
        use_auto_chunks = True
        manual_chunks = None
        final_system_type = system_type
        final_recommendations = recommendations
    

    usable_memory_gb = available_memory_gb  # Use all available memory 
    memory_per_worker_gb = usable_memory_gb / nworkers
    memory_per_worker_bytes = memory_per_worker_gb * (1024**3)
    
    print(f"\n{'='*60}")
    print(f"FINAL CONFIGURATION")
    print(f"{'='*60}")
    print(f"System type: {final_system_type.upper()}")
    print(f"Workers: {nworkers}")
    print(f"Total RAM: {total_memory_gb:.1f} GB")
    print(f"Available RAM: {available_memory_gb:.1f} GB")
    
    # Get data dimensions
    with xr.open_mfdataset(pr_file) as ds:
        time_size = len(ds.time.sel(time=slice(year_start, year_end)))
        lat_size = len(ds.lat)
        lon_size = len(ds.lon)
    
    print(f"Data dimensions: {time_size} × {lat_size} × {lon_size}")
    
    # Calculate chunk sizes
    if use_auto_chunks:
        # Calculate target number of chunks based on workers
        # Target close to but less than 3 chunks per worker for optimal efficiency
        min_target_chunks = int(nworkers * 2.0)   # Minimum 2 chunks per worker
        max_target_chunks = int(nworkers * target_chunks_per_worker)  # Cap at 3 per worker
        target_total_chunks = int(nworkers * target_chunks_per_worker)
        
        # Clamp target to the valid range
        target_total_chunks = max(min_target_chunks, min(target_total_chunks, max_target_chunks))
        effective_target_ratio = target_total_chunks / nworkers
        
        # Memory estimation for KBDI calculation
        # Arrays needed simultaneously: pr, tmax, KBDI, pnet, rr, cat, mean_ann_pr, day_int
        # Plus temporary arrays during computation
        arrays_per_gridpoint_per_timestep = 10  # Conservative estimate
        bytes_per_gridpoint_per_timestep = 4 * arrays_per_gridpoint_per_timestep  # float32 = 4 bytes
        
        # Calculate memory constraint (only for chunk sizing, not worker limits)
        effective_memory_per_worker = memory_per_worker_bytes * 0.8
        # if final_system_type == 'hpc':
        #     effective_memory_per_worker = memory_per_worker_bytes * 0.8  # 80% for chunk calculation only
        # else:
        #     effective_memory_per_worker = memory_per_worker_bytes * 0.7  # 60% for single machine
        
        # Maximum spatial points per chunk based on memory
        max_spatial_points_memory = effective_memory_per_worker / (time_size * bytes_per_gridpoint_per_timestep)
        
        # Calculate chunk sizes to meet both memory and parallelization constraints
        total_spatial_points = lat_size * lon_size
        target_spatial_points_per_chunk = total_spatial_points / target_total_chunks
        
        # Use the more restrictive constraint
        max_spatial_points_per_chunk = min(max_spatial_points_memory, target_spatial_points_per_chunk)
        
        # Calculate initial chunk dimensions
        target_chunk_dim = int(np.sqrt(max_spatial_points_per_chunk))
        
        # Set reasonable bounds based on system type and data size
        if final_system_type == 'hpc':
            min_chunk_size = max(8, min(16, lat_size//10, lon_size//10))  # Reasonable minimum
            max_chunk_size = min(64, lat_size//2, lon_size//2)  # Allow larger chunks for fewer total chunks
        else:
            min_chunk_size = max(4, min(8, lat_size//4, lon_size//4))
            max_chunk_size = min(32, lat_size//2, lon_size//2)
        
        # Constrain chunk dimension
        target_chunk_dim = max(min_chunk_size, min(target_chunk_dim, max_chunk_size))
        
        # Find optimal chunk sizes from preferred values
        preferred_sizes = [8, 10, 12, 16, 20, 24, 32, 40, 48, 60, 64]  # Include larger sizes
        preferred_sizes = [s for s in preferred_sizes if min_chunk_size <= s <= max_chunk_size]
        
        # Choose lat and lon chunk sizes
        lat_chunk = min(preferred_sizes, key=lambda x: abs(x - target_chunk_dim))
        lon_chunk = min(preferred_sizes, key=lambda x: abs(x - target_chunk_dim))
        
        # Adjust chunk sizes to optimize for the target ratio (2.0 to 2.95 per worker)
        best_chunk_size = None
        best_chunks_per_worker = 0
        
        # Try all combinations of chunk sizes to find optimal configuration
        for lat_size_test in preferred_sizes:
            for lon_size_test in preferred_sizes:
                # Check if this size creates reasonable number of chunks
                lat_chunks_count = (lat_size + lat_size_test - 1) // lat_size_test
                lon_chunks_count = (lon_size + lon_size_test - 1) // lon_size_test
                total_chunks_with_size = lat_chunks_count * lon_chunks_count
                chunks_per_worker_with_size = total_chunks_with_size / nworkers
                
                # Check if remainder chunks are not too small
                lat_remainder = lat_size % lat_size_test
                lon_remainder = lon_size % lon_size_test
                lat_remainder_ok = lat_remainder == 0 or lat_remainder >= lat_size_test // 3
                lon_remainder_ok = lon_remainder == 0 or lon_size_test >= lon_size_test // 3
                
                # Check memory constraint for this chunk size
                points_per_chunk_test = lat_size_test * lon_size_test
                memory_per_chunk_test = (points_per_chunk_test * time_size * 4 * arrays_per_gridpoint_per_timestep) / (1024**3)
                memory_ok = memory_per_chunk_test <= memory_per_worker_gb * 0.8  # 80% memory limit
                
                # Prefer configurations that stay within 2.0-2.95 chunks per worker
                if (chunks_per_worker_with_size >= 2.0 and 
                    chunks_per_worker_with_size <= target_chunks_per_worker and
                    lat_remainder_ok and lon_remainder_ok and
                    memory_ok):
                    # Prefer configurations closer to the target ratio
                    target_distance = abs(chunks_per_worker_with_size - effective_target_ratio)
                    best_distance = abs(best_chunks_per_worker - effective_target_ratio) if best_chunk_size else float('inf')
                    
                    if target_distance < best_distance:
                        best_chunk_size = (lat_size_test, lon_size_test)
                        best_chunks_per_worker = chunks_per_worker_with_size
        
        # Use the best size found
        if best_chunk_size is not None:
            lat_chunk, lon_chunk = best_chunk_size
        else:
            # Fallback: find size that gives chunks per worker <= 2.95
            for lat_size_test in sorted(preferred_sizes, reverse=True):
                for lon_size_test in sorted(preferred_sizes, reverse=True):
                    lat_chunks_count = (lat_size + lat_size_test - 1) // lat_size_test
                    lon_chunks_count = (lon_size + lon_size_test - 1) // lon_size_test
                    total_chunks_with_size = lat_chunks_count * lon_chunks_count
                    chunks_per_worker_with_size = total_chunks_with_size / nworkers
                    
                    # Check memory constraint
                    points_per_chunk_test = lat_size_test * lon_size_test
                    memory_per_chunk_test = (points_per_chunk_test * time_size * 4 * arrays_per_gridpoint_per_timestep) / (1024**3)
                    memory_ok = memory_per_chunk_test <= memory_per_worker_gb * 0.8
                    
                    if chunks_per_worker_with_size <= target_chunks_per_worker and memory_ok:
                        lat_chunk, lon_chunk = lat_size_test, lon_size_test
                        break
                if chunks_per_worker_with_size <= target_chunks_per_worker and memory_ok:
                    break
        
        # Ensure we don't exceed data dimensions
        lat_chunk = min(lat_chunk, lat_size)
        lon_chunk = min(lon_chunk, lon_size)
        
        chunks = {'time': -1, 'lat': lat_chunk, 'lon': lon_chunk}
        
        # Calculate actual number of chunks
        actual_lat_chunks = (lat_size + lat_chunk - 1) // lat_chunk
        actual_lon_chunks = (lon_size + lon_chunk - 1) // lon_chunk
        actual_total_chunks = actual_lat_chunks * actual_lon_chunks
        
        # If we still don't have a good configuration, try to get under 2.95 chunks per worker
        if actual_total_chunks / nworkers > target_chunks_per_worker:            
            # Try progressively larger chunk sizes
            for lat_size_test in sorted([s for s in preferred_sizes if s > lat_chunk], reverse=True):
                for lon_size_test in sorted([s for s in preferred_sizes if s > lon_chunk], reverse=True):
                    test_lat_chunks = (lat_size + lat_size_test - 1) // lat_size_test
                    test_lon_chunks = (lon_size + lon_size_test - 1) // lon_size_test
                    test_total_chunks = test_lat_chunks * test_lon_chunks
                    test_chunks_per_worker = test_total_chunks / nworkers
                    
                    # Check memory constraint
                    points_per_chunk_test = lat_size_test * lon_size_test
                    memory_per_chunk_test = (points_per_chunk_test * time_size * 4 * arrays_per_gridpoint_per_timestep) / (1024**3)
                    memory_ok = memory_per_chunk_test <= memory_per_worker_gb * 0.8
                    
                    if test_chunks_per_worker <= target_chunks_per_worker and test_chunks_per_worker >= 2.0 and memory_ok:
                        lat_chunk, lon_chunk = lat_size_test, lon_size_test
                        actual_lat_chunks = test_lat_chunks
                        actual_lon_chunks = test_lon_chunks
                        actual_total_chunks = test_total_chunks
                        print(f"   Found better configuration: {lat_chunk}×{lon_chunk} = {test_chunks_per_worker:.1f} chunks/worker")
                        break
                if test_chunks_per_worker <= target_chunks_per_worker and memory_ok:
                    break
            
            chunks = {'time': -1, 'lat': lat_chunk, 'lon': lon_chunk}
        
    else:
        chunks = manual_chunks
        lat_chunk = chunks['lat']
        lon_chunk = chunks['lon']
        
        # Calculate actual number of chunks for manual configuration
        actual_lat_chunks = (lat_size + lat_chunk - 1) // lat_chunk
        actual_lon_chunks = (lon_size + lon_chunk - 1) // lon_chunk
        actual_total_chunks = actual_lat_chunks * actual_lon_chunks
    
    # Calculate memory usage per chunk
    points_per_chunk = lat_chunk * lon_chunk
    arrays_per_chunk = 10  # Conservative estimate for KBDI calculation
    memory_per_chunk_gb = (points_per_chunk * time_size * 4 * arrays_per_chunk) / (1024**3)
    
    # Calculate chunk-to-worker ratio
    chunks_per_worker = actual_total_chunks / nworkers
    
    print(f"Chunk sizes: time={chunks['time']}, lat={lat_chunk}, lon={lon_chunk}")
    print(f"Spatial points per chunk: {points_per_chunk}")    
    print(f"Target chunks per worker: {effective_target_ratio:.1f} (if CPU/RAM allows)")
    print(f"Chunks per worker: {chunks_per_worker:.1f}")
    print(f"Total chunks: {actual_total_chunks} ({actual_lat_chunks} × {actual_lon_chunks})")
    print(f"{'='*60}\n")
    
    # Set Dask memory management configuration (same for all systems)
    dask.config.set({
        'distributed.worker.memory.spill': 0.85,    # Spill to disk at 85% worker memory
        'distributed.worker.memory.target': target_mem_percent,   # Target 75% worker memory usage
        'distributed.worker.memory.terminate': 0.95, # Terminate worker at 95% worker memory
        'distributed.worker.memory.pause': 0.85,     # Pause accepting tasks at 85% worker memory
    })
        
    return {
        'nworkers': nworkers,
        'chunks': chunks,
        'system_info': {
            'system_type': final_system_type,
            'cpu_count': cpu_count,
            'total_memory_gb': total_memory_gb,
            'available_memory_gb': available_memory_gb,
            'data_dimensions': (time_size, lat_size, lon_size),
            'actual_total_chunks': actual_total_chunks,
            'chunks_per_worker': chunks_per_worker,   
        }
    }


def quick_configure(pr_file, tmax_file, year_start, year_end):
    """
    Quick configuration without prompts (for batch jobs)
    """
    return auto_configure_processing(pr_file, tmax_file, year_start, year_end, 
                                   interactive=False)