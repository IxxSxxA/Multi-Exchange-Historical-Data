# utils/inspect_parquet.py

import os
import sys
import glob
import pandas as pd

# Add project root to sys.path to ensure utils module is found
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.file_utils import inspect_parquet
from utils.logger import setup_logger

def display_head_tail(df, logger, num_rows=5):
    """Display the first and last num_rows of the DataFrame with a separator."""
    if df.empty:
        logger.info("Nessun dato disponibile nel DataFrame.")
        return
    
    logger.info(f"Prime {num_rows} righe:")
    logger.info(df[['datetime', 'open', 'high', 'low', 'close', 'volume']].head(num_rows).to_string(index=False))
    logger.info("."*50)
    logger.info(f"Ultime {num_rows} righe:")
    logger.info(df[['datetime', 'open', 'high', 'low', 'close', 'volume']].tail(num_rows).to_string(index=False))

def main():
    # Setup logger
    logger = setup_logger('logs')  # Pass only the logs directory
    
    # Define directory containing Parquet files
    candles_dir = os.path.join(project_root, 'data', 'candles')
    
    # Print header
    logger.info("="*50)
    logger.info(f"Ispeziono cartella: {candles_dir}")
    logger.info("="*50 + "\n")
    
    # Check if directory exists
    if not os.path.exists(candles_dir):
        logger.error(f"La cartella {candles_dir} non esiste.")
        return
    
    # Find all Parquet files in the directory
    parquet_files = glob.glob(os.path.join(candles_dir, '*.parquet'))
    
    if not parquet_files:
        logger.info(f"Nessun file .parquet trovato in {candles_dir}.")
        return
    
    logger.info(f"Trovati n° {len(parquet_files)} file .parquet\n")
    # logger.info("-"*50 + "\n")
    
    # Inspect each Parquet file
    for filepath in parquet_files:
        filename = os.path.basename(filepath)
        logger.info("-"*50 + "\n")
        logger.info(f"Eseguo tests sul file: {filename}\n")
        
        
        
        # Load Parquet file for head/tail display
        df = pd.read_parquet(filepath) if os.path.exists(filepath) else pd.DataFrame()
        if not df.empty:
            # Convert timestamp_ms to datetime for readability
            df['datetime'] = df['timestamp_ms'].apply(lambda x: pd.to_datetime(x, unit='ms'))
            display_head_tail(df, logger)
            logger.info("-"*50)
        
        # Run inspection
        success = inspect_parquet(filepath, logger)
        if success:
            logger.info(f"Ispezione del file {filename} completata con successo.")
        else:
            logger.error(f"Ispezione del file {filename} fallita.")
        logger.info("-"*50 + "\n")
    
    logger.info("Fine Tests")
    logger.info("="*50)

if __name__ == "__main__":
    main()