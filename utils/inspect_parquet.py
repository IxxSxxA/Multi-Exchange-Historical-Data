# utils/file_utils.py
import pandas as pd
import os
from utils.date_utils import timestamp_to_datetime

def get_parquet_filename(exchange_id, pair, timeframe, market_type, data_type='candles'):
    """Genera il nome del file Parquet con struttura organizzata."""
    pair_safe = pair.replace('/', '-').replace(':', '-')
    
    if data_type == 'candles':
        return f"{exchange_id}_{market_type}_{pair_safe}_{timeframe}.parquet"
    elif data_type == 'funding':
        return f"{exchange_id}_{market_type}_{pair_safe}_funding.parquet"
    elif data_type == 'oi':  # Open Interest
        return f"{exchange_id}_{market_type}_{pair_safe}_oi.parquet"
    else:
        return f"{exchange_id}_{market_type}_{pair_safe}_{data_type}.parquet"

def get_data_directory(base_path, market_type, data_type):
    """Restituisce il percorso organizzato per i dati."""
    # Costruisci il percorso manualmente per evitare problemi di tipo
    return f"{base_path}/{market_type}/{data_type}"

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
    """Salva DataFrame in Parquet con gestione append."""
    if append and check_file_exists(path):
        existing_df = load_parquet(path)
        if not existing_df.empty and not df.empty:
            # Usa tutte le colonne per il drop_duplicates per sicurezza
            df = pd.concat([existing_df, df]).drop_duplicates().sort_values('timestamp_ms' if 'timestamp_ms' in df.columns else df.columns[0])
    
    # Assicurati che la directory esista
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"Salvati {len(df)} record in {path}")

def ensure_directory_exists(directory):
    """Assicura che la directory esista."""
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

def inspect_parquet(filepath, logger=None):
    """Ispeziona un file Parquet e restituisce un riepilogo dei dati."""
    output = logger.info if logger else print
    
    if not check_file_exists(filepath):
        output(f"Errore: Il file {filepath} non esiste.")
        return False
    
    # Carica il file Parquet
    df = load_parquet(filepath)
    
    if df.empty:
        output(f"Il file {filepath} è vuoto.")
        return False
    
    # Determina il tipo di dati in base al nome del file
    filename = os.path.basename(filepath)
    
    if 'funding' in filename:
        # Ispezione per file funding
        output(f"📊 ISPEZIONE FUNDING RATE: {filename}")
        output(f"Numero di record funding: {len(df)}")
        
        if 'timestamp_ms' in df.columns:
            df['datetime'] = df['timestamp_ms'].apply(timestamp_to_datetime)
            output(f"Data iniziale: {df['datetime'].min()}")
            output(f"Data finale: {df['datetime'].max()}")
            
            # Statistiche funding rate
            if 'funding_rate' in df.columns:
                output(f"Funding rate min: {df['funding_rate'].min():.6f}")
                output(f"Funding rate max: {df['funding_rate'].max():.6f}")
                output(f"Funding rate mean: {df['funding_rate'].mean():.6f}")
                
            output("Ultimi 5 record funding:")
            display_columns = ['datetime', 'funding_rate'] if 'funding_rate' in df.columns else ['datetime']
            output(df[display_columns].tail(5).to_string(index=False))
            
    elif 'oi' in filename:
        # Ispezione per file open interest
        output(f"📊 ISPEZIONE OPEN INTEREST: {filename}")
        output(f"Numero di record OI: {len(df)}")
        
        if 'timestamp_ms' in df.columns:
            df['datetime'] = df['timestamp_ms'].apply(timestamp_to_datetime)
            output(f"Data iniziale: {df['datetime'].min()}")
            output(f"Data finale: {df['datetime'].max()}")
            
            # Statistiche open interest
            if 'open_interest' in df.columns:
                output(f"Open Interest min: {df['open_interest'].min():.2f}")
                output(f"Open Interest max: {df['open_interest'].max():.2f}")
                output(f"Open Interest mean: {df['open_interest'].mean():.2f}")
                
            output("Ultimi 5 record open interest:")
            display_columns = ['datetime', 'open_interest'] if 'open_interest' in df.columns else ['datetime']
            output(df[display_columns].tail(5).to_string(index=False))
            
    else:
        # Ispezione per file candele (default)
        output(f"📊 ISPEZIONE CANDLE: {filename}")
        
        # Converti timestamp_ms in datetime per leggibilità
        if 'timestamp_ms' in df.columns:
            df['datetime'] = df['timestamp_ms'].apply(timestamp_to_datetime)
        
        # Stampa informazioni di base
        output(f"Numero di candele: {len(df)}")
        
        if 'datetime' in df.columns:
            output(f"Data iniziale: {df['datetime'].min()}")
            output(f"Data finale: {df['datetime'].max()}")
        
        # Colonne disponibili
        output(f"Colonne disponibili: {list(df.columns)}")
        
        # Stampa le ultime 5 candele
        output("Ultime 5 candele:")
        candle_columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        available_columns = [col for col in candle_columns if col in df.columns]
        if 'trades_count' in df.columns:
            available_columns.append('trades_count')
        
        if available_columns:
            output(df[available_columns].tail(5).to_string(index=False))
        
        # Controlla i gap nei timestamp (per timeframe 1m, ogni candela dovrebbe essere a +60 secondi)
        if 'timestamp_ms' in df.columns:
            df_sorted = df.sort_values('timestamp_ms').reset_index(drop=True)
            df_sorted['timestamp_diff'] = df_sorted['timestamp_ms'].diff()
            gaps = df_sorted[df_sorted['timestamp_diff'] > 60000]  # > 1 minuto
            
            if not gaps.empty:
                output("⚠️  GAP RILEVATI (differenza > 1 minuto):")
                for idx in gaps.index:
                    if idx > 0:  # Assicurati che ci sia un elemento precedente
                        prev_row = df_sorted.iloc[idx - 1]
                        curr_row = df_sorted.iloc[idx]
                        prev_time = timestamp_to_datetime(prev_row['timestamp_ms'])
                        curr_time = timestamp_to_datetime(curr_row['timestamp_ms'])
                        gap_seconds = curr_row['timestamp_diff'] / 1000
                        output(f"Gap tra {prev_time} e {curr_time} ({gap_seconds:.1f} secondi)")
            else:
                output("✅ Nessun gap rilevato nei dati (timeline continua)")
    
    return True