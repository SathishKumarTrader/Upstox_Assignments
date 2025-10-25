"""
Batch download of historical data for multiple symbols.

This script demonstrates how to download historical data for multiple symbols
in a batch process with automatic date range splitting for large periods.
"""

import os
import time
import logging
import importlib
from typing import List, Dict, Tuple
from datetime import datetime, timedelta
import pandas as pd

from symbol_mapper import SymbolMapper
from download_historical_data import HistoricalDataDownloader
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def split_date_range(start_date: str, end_date: str, chunk_days: int = 30) -> List[Tuple[str, str]]:
    """
    Split a date range into smaller chunks of specified days.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        chunk_days: Number of days per chunk (default: 30)
        
    Returns:
        List of (start_date, end_date) tuples for each chunk
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    chunks = []
    current_start = start_dt
    
    while current_start <= end_dt:
        current_end = min(current_start + timedelta(days=chunk_days - 1), end_dt)
        chunks.append((
            current_start.strftime("%Y-%m-%d"),
            current_end.strftime("%Y-%m-%d")
        ))
        current_start = current_end + timedelta(days=1)
    
    return chunks

def download_symbol_data_chunked(symbol: str, config_module=None, symbol_mapper=None) -> str:
    """
    Download historical data for a symbol using date chunking.
    
    Args:
        symbol: Symbol to download data for
        config_module: Config module containing date range and other settings
        symbol_mapper: Optional SymbolMapper instance to reuse (avoids reloading data)
        
    Returns:
        Path to the combined output file, or empty string if failed
    """
    if config_module is None:
        config_module = config
    
    if symbol_mapper is None:
        symbol_mapper = SymbolMapper()
    
    try:
        # Get date range from config
        start_date = config_module.START_DATE
        end_date = config_module.END_DATE
        
        # Split into 30-day chunks
        date_chunks = split_date_range(start_date, end_date, 30)
        
        logger.info(f"Downloading {symbol} data in {len(date_chunks)} chunks from {start_date} to {end_date}")
        
        all_data = []
        
        for i, (chunk_start, chunk_end) in enumerate(date_chunks, 1):
            logger.info(f"Processing chunk {i}/{len(date_chunks)}: {chunk_start} to {chunk_end}")
            
            # Download data for this chunk
            chunk_data = download_chunk_data(symbol, chunk_start, chunk_end, config_module, symbol_mapper)
            
            if chunk_data is not None and not chunk_data.empty:
                all_data.append(chunk_data)
            
            # Add delay between API calls to avoid rate limiting
            time.sleep(1)
        
        if all_data:
            # Combine all chunks
            combined_data = pd.concat(all_data, ignore_index=True)
            
            # Sort by timestamp if available
            if 'timestamp' in combined_data.columns:
                combined_data = combined_data.sort_values('timestamp')
            elif 'date' in combined_data.columns:
                combined_data = combined_data.sort_values('date')
            
            # Save combined data
            output_dir = getattr(config_module, 'OUTPUT_DIRECTORY', 'historical_data')
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            output_file = os.path.join(output_dir, f"{symbol}_{start_date}_to_{end_date}.csv")
            combined_data.to_csv(output_file, index=False)
            
            logger.info(f"Successfully saved combined data for {symbol} to {output_file}")
            return output_file
        else:
            logger.error(f"No data retrieved for {symbol}")
            return ""
            
    except Exception as e:
        logger.exception(f"Error downloading data for {symbol}: {str(e)}")
        return ""

def download_chunk_data(symbol: str, start_date: str, end_date: str, config_module, symbol_mapper: SymbolMapper) -> pd.DataFrame:
    """
    Download data for a specific date chunk using HistoricalDataDownloader.
    
    Args:
        symbol: Trading symbol
        start_date: Start date for the chunk (YYYY-MM-DD)
        end_date: End date for the chunk (YYYY-MM-DD)
        config_module: Configuration module with API settings
        symbol_mapper: SymbolMapper instance to reuse (avoids reloading data)
        
    Returns:
        DataFrame with the downloaded data (columns should include timestamp/date, open, high, low, close, volume)
    """
    try:
        logger.info(f"Downloading {symbol} from {start_date} to {end_date}")
        
        # Create a temporary config module for this chunk
        class ChunkConfig:
            def __init__(self, base_config, symbol, start_date, end_date):
                # Copy all attributes from base config
                for attr in dir(base_config):
                    if not attr.startswith('_'):
                        setattr(self, attr, getattr(base_config, attr))
                
                # Override specific attributes for this chunk
                self.SYMBOL = symbol
                self.START_DATE = start_date
                self.END_DATE = end_date
        
        chunk_config = ChunkConfig(config_module, symbol, start_date, end_date)
        
        # Create downloader for this chunk with reused symbol_mapper
        downloader = HistoricalDataDownloader(config_module=chunk_config, symbol_mapper=symbol_mapper)
        
        # Download the data
        data = downloader.download_historical_data()
        
        if data is not None and not data.empty:
            logger.info(f"Successfully downloaded {len(data)} records for {symbol} ({start_date} to {end_date})")
            return data
        else:
            logger.warning(f"No data returned for {symbol} ({start_date} to {end_date})")
            return pd.DataFrame()
            
    except Exception as e:
        logger.exception(f"Error downloading chunk data for {symbol} ({start_date} to {end_date}): {str(e)}")
        return pd.DataFrame()

def download_multiple_symbols(symbols: List[str], config_module=None) -> Dict[str, str]:
    """
    Download historical data for multiple symbols using date chunking.
    
    Args:
        symbols: List of symbols to download data for
        config_module: Config module to use for download settings
        
    Returns:
        Dictionary mapping symbols to output filepaths
    """
    if config_module is None:
        config_module = config
        
    # Create symbol mapper once for efficiency
    symbol_mapper = SymbolMapper()
    
    results = {}
    
    for symbol in symbols:
        try:
            logger.info(f"Processing symbol: {symbol}")
            
            # Download data using chunked approach with shared symbol_mapper
            output_file = download_symbol_data_chunked(symbol, config_module, symbol_mapper)
            
            if output_file:
                results[symbol] = output_file
                logger.info(f"Successfully downloaded data for {symbol}")
            else:
                logger.error(f"Failed to download data for {symbol}")
                results[symbol] = ""
                
            # Add a small delay to avoid API rate limiting
            time.sleep(1)
            
        except Exception as e:
            logger.exception(f"Error processing {symbol}: {str(e)}")
            results[symbol] = ""
            
    return results

def download_from_list_file(file_path: str, config_module=None) -> Dict[str, str]:
    """
    Download historical data for symbols listed in a text file.
    
    Args:
        file_path: Path to a text file with one symbol per line
        config_module: Config module to use for download settings
        
    Returns:
        Dictionary mapping symbols to output filepaths
    """
    try:
        with open(file_path, 'r') as f:
            symbols = [line.strip() for line in f if line.strip()]
            
        return download_multiple_symbols(symbols, config_module)
    
    except Exception as e:
        logger.exception(f"Error reading symbol list file: {str(e)}")
        return {}

if __name__ == "__main__":
    # Example: Download data for multiple symbols
    symbols_to_download = ["SBIN", "RELIANCE", "TCS", "INFY"]
    
    results = download_from_list_file("symbols.txt")
    # print(f"Starting batch download for {len(symbols_to_download)} symbols")
    # results = download_multiple_symbols(symbols_to_download)
    
    # Print summary
    success_count = sum(1 for filepath in results.values() if filepath)
    print(f"Download summary: {success_count}/{len(results)} symbols successful")
    
    for symbol, filepath in results.items():
        status = "SUCCESS" if filepath else "FAILED"
        print(f"  {symbol}: {status}")
    
    # Example: To download from a list file, uncomment below:
    # results = download_from_list_file("symbols.txt")
