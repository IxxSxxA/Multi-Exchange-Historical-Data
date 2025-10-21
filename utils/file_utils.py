# utils/file_utils.py
import pandas as pd
import os
from utils.date_utils import timestamp_to_datetime

def get_parquet_filename(exchange_id, pair, timeframe):
    """Genera il nome del file Parquet."""
    pair_safe = pair.replace('/', '-')
    return f"{exchange_id}_{pair_safe}_{timeframe}.parquet"

def check_file_exists(path):
    """Controlla se un file esiste."""
    return os.path.exists(path)

def load_parquet(path):
    """Carica un file Parquet."""
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()

def save_parquet(df, path, append=False):
    if append and check_file_exists(path):
        existing_df = load_parquet(path)
        df = pd.concat([existing_df, df]).drop_duplicates(subset=['timestamp_ms']).sort_values('timestamp_ms')
    df.to_parquet(path, index=False)
    print(f"Salvati {len(df)} candele in {path}")  # Log opzionale per debug

def ensure_directory_exists(directory):
    """Assicura che la directory esista."""
    if not os.path.exists(directory):
        os.makedirs(directory)

def inspect_parquet(filepath, logger=None):
    """Ispeziona un file Parquet e restituisce un riepilogo dei dati.
    
    Args:
        filepath (str): Percorso del file Parquet.
        logger: Oggetto logger per output (opzionale, default: print).
    
    Returns:
        bool: True se l'ispezione è riuscita, False altrimenti.
    """
    output = logger.info if logger else print
    
    if not check_file_exists(filepath):
        output(f"Errore: Il file {filepath} non esiste.")
        return False
    
    # Carica il file Parquet
    df = load_parquet(filepath)
    
    if df.empty:
        output(f"Il file {filepath} è vuoto.")
        return False
    
    # Converti timestamp_ms in datetime per leggibilità
    df['datetime'] = df['timestamp_ms'].apply(timestamp_to_datetime)
    
    # Stampa per debug in caso serva
    # print(f"Min timestamp: {df['timestamp_ms'].min()}, Max timestamp: {df['timestamp_ms'].max()}")
    
    # Stampa informazioni di base utili per debug
    # output(f"File: {os.path.basename(filepath)}")
    # output(f"Numero di candele: {len(df)}")
    # output(f"Data iniziale: {df['datetime'].min()}")
    # output(f"Data finale: {df['datetime'].max()}")
    
    # Stampa le ultime 5 candele
    # output("Ultime 5 candele:")sta    
    # output(df[['datetime', 'open', 'high', 'low', 'close', 'volume']].tail(5))
    
    # Controlla i gap nei timestamp (per timeframe 1m, ogni candela dovrebbe essere a +60 secondi)
    df['timestamp_diff'] = df['timestamp_ms'].diff()
    gaps = df[df['timestamp_diff'] > 60000]
    if not gaps.empty:
        # SE CI SONO GAP allora siamo qui

        output("Gap rilevati (differenza > 1 minuto):")
        for idx, row in gaps.iterrows():
            prev_time = timestamp_to_datetime(df.at[idx-1, 'timestamp_ms'])
            curr_time = timestamp_to_datetime(row['timestamp_ms'])
            output(f"Gap tra {prev_time} e {curr_time} ({row['timestamp_diff']/1000} secondi)")
    else:
        # SE NON CI SONO GAP allora siamo qui
        
        # Stampa informazioni di base utili per debug

        output(f"File: {os.path.basename(filepath)}")
        output(f"Numero di candele: {len(df)}")
        output(f"Data iniziale: {df['datetime'].min()}")
        output(f"Data finale: {df['datetime'].max()}")
    
        # Stampa le ultime 5 candele
        output("Nessun gap rilevato nei dati.")
        output("Ultime 5 candele:")
        output(df[['datetime', 'open', 'high', 'low', 'close', 'volume']].tail(5))
        
    
    return True