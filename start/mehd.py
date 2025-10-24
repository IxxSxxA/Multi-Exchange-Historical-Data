# start/mehd.py
import ccxt
import pandas as pd
import time
import os
import sys
from colorama import init, Fore, Style

# Inizializza colorama
init(autoreset=True)

# Import da utils e config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.date_utils import parse_date, get_current_timestamp_ms, timestamp_to_datetime
from utils.file_utils import get_parquet_filename, check_file_exists, load_parquet, save_parquet, ensure_directory_exists, inspect_parquet
from utils.market_utils import detect_market_type, get_available_pairs, format_volume_display
from utils.logger import setup_logger
from start.config import SUPPORTED_EXCHANGES, DEFAULT_EXCHANGE, DEFAULT_ASSET, TIMEFRAME, DATA_PATH, LOGS_PATH, FETCH_FUNDING, FETCH_OPEN_INTEREST, DATA_DIRECTORIES, DEFAULT_START_DATE, FUNDING_TIMEFRAME, OI_TIMEFRAME

def validate_exchange(exchange_name):
    """Valida se l'exchange è supportato e raggiungibile."""
    if exchange_name not in SUPPORTED_EXCHANGES:
        raise ValueError(f"Exchange '{exchange_name}' non supportato. Supportati: {SUPPORTED_EXCHANGES}")
    try:
        exchange_class = getattr(ccxt, exchange_name, None)
        if exchange_class is None:
            raise ValueError(f"Exchange '{exchange_name}' non trovato in CCXT")
        exchange = exchange_class()
        logger.info(f"Connessione a {exchange_name} in corso...")
        markets = exchange.load_markets()
        if markets is None:
            raise ValueError(f"Impossibile caricare i mercati per {exchange_name}: risposta None")
        logger.info(f"✅ Mercati caricati per {exchange_name}, trovati {len(markets)} mercati")
        return exchange
    except Exception as e:
        logger.error(f"Errore dettagliato nella connessione a {exchange_name}: {str(e)}")
        raise ValueError(f"Errore nella connessione all'exchange {exchange_name}: {str(e)}")

def get_oldest_timestamp(exchange, pair, market_type='spot'):
    """Ottiene il timestamp più vecchio disponibile per il pair."""
    try:
        # Prova a ottenere le candele più vecchie disponibili
        ohlcv = exchange.fetch_ohlcv(pair, TIMEFRAME, since=0, limit=1)
        if ohlcv:
            return ohlcv[0][0]
    except Exception as e:
        logger.warning(f"Impossibile determinare timestamp più vecchio: {e}")
    
    # Default: usa la data di default dal config
    return parse_date(DEFAULT_START_DATE)

def fetch_ohlcv(exchange, pair, tf, since, limit=1000):
    """Fetch OHLCV con gestione rate limits."""
    try:
        return exchange.fetch_ohlcv(pair, tf, since=since, limit=limit)
    except ccxt.NetworkError as e:
        logger.error(f"Errore di rete durante il fetch: {e}")
        time.sleep(5)
        return []
    except ccxt.ExchangeError as e:
        logger.error(f"Errore exchange durante il fetch: {e}")
        return []
    except Exception as e:
        logger.error(f"Errore imprevisto durante il fetch: {e}")
        return []

def download_ohlcv_data(exchange, pair, market_type, filepath, start_timestamp=None, append=False):
    """Scarica dati OHLCV e salva in Parquet."""
    data = []
    limit = 1000
    batch_save_size = 10
    batch_count = 0
    
    # Gestisci append in base all'esistenza del file
    if append and os.path.exists(filepath):
        existing_df = load_parquet(filepath)
        if not existing_df.empty:
            # Prendi l'ultimo timestamp dal file esistente
            last_timestamp = existing_df['timestamp_ms'].max()
            since = last_timestamp + 1  # Continua dal successivo
            logger.info(f"🔄 Continuando da {timestamp_to_datetime(last_timestamp)}")
        else:
            # File esiste ma è vuoto, usa start_timestamp se fornito, altrimenti DEFAULT_START_DATE
            since = start_timestamp if start_timestamp else parse_date(DEFAULT_START_DATE)
            logger.info(f"🔄 File esistente vuoto, partendo da {timestamp_to_datetime(since)}")
    else:
        # Modalità overwrite o file non esistente
        # Usa start_timestamp se fornito, altrimenti DEFAULT_START_DATE
        since = start_timestamp if start_timestamp else parse_date(DEFAULT_START_DATE)
        logger.info(f"🔄 Partendo da {timestamp_to_datetime(since)}")
    
    latest_timestamp = get_current_timestamp_ms()
    
    try:
        total_candles = 0
        logger.info(f"📥 Scaricando dati OHLCV per {pair}...")
        
        while since < latest_timestamp:
            ohlcv = fetch_ohlcv(exchange, pair, TIMEFRAME, since, limit)
            if not ohlcv:
                logger.info("✅ Nessun dato aggiuntivo disponibile.")
                break
            
            data.extend(ohlcv)
            since = ohlcv[-1][0] + 1  # +1 per evitare duplicati
            total_candles += len(ohlcv)
            batch_count += 1
            
            # Rate limiting rispettoso
            time.sleep(exchange.rateLimit / 1000)
            
            # Salva in batch per robustezza
            if batch_count >= batch_save_size:
                logger.info(f"💾 Salvataggio intermedio di {len(data)} candele (totale: {total_candles})...")
                df_batch = create_ohlcv_dataframe(data)
                save_parquet(df_batch, filepath, append=append)
                data = []
                batch_count = 0
                append = True  # Dopo il primo salvataggio, usa append
        
        # Salva l'ultimo batch
        if data:
            logger.info(f"💾 Salvataggio finale di {len(data)} candele (totale: {total_candles})...")
            df_final = create_ohlcv_dataframe(data)
            save_parquet(df_final, filepath, append=append)
        
        # Carica e restituisci il DataFrame completo
        if os.path.exists(filepath):
            df = load_parquet(filepath)
            logger.info(f"✅ Dati caricati da {os.path.basename(filepath)} - Totale candele: {len(df)}")
            return df
        else:
            logger.warning("❌ File non trovato dopo salvataggio.")
            return None
    
    except Exception as e:
        logger.error(f"❌ Errore durante il download OHLCV: {e}")
        return None

def create_ohlcv_dataframe(data):
    """Crea DataFrame dalle candele OHLCV."""
    if not data:
        return pd.DataFrame()
    
    # Colonne base OHLCV
    columns = ['timestamp_ms', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(data, columns=columns[:len(data[0])])
    
    # Aggiungi trades_count se disponibile
    if len(data[0]) > 6:
        df['trades_count'] = [row[6] for row in data]
    
    return df

# def download_funding_data(exchange, pair, filepath, start_timestamp=None, append=False):
#     """Scarica dati di funding rate per perpetual."""
    
#     funding_timeframe = FUNDING_TIMEFRAME  # Usa il timeframe dal config
    
#     logger.info(f"📥 Scaricando funding rate per {pair} (TF: {funding_timeframe})...")
    
#     data = []
#     since = start_timestamp if start_timestamp else get_oldest_timestamp(exchange, pair, 'perpetual')
#     latest_timestamp = get_current_timestamp_ms()

#     if append and os.path.exists(filepath):
#         existing_df = load_parquet(filepath)
#         if not existing_df.empty:
#             last_timestamp = existing_df['timestamp_ms'].max()
#             since = last_timestamp + 1
#             logger.info(f"🔄 Funding: continuando da {timestamp_to_datetime(last_timestamp)}")
#         else:
#             since = start_timestamp if start_timestamp else get_oldest_timestamp(exchange, pair, 'perpetual')
#     else:
#         since = start_timestamp if start_timestamp else get_oldest_timestamp(exchange, pair, 'perpetual')    
    
#     try:
#         total_records = 0
#         while since < latest_timestamp:
#             try:
#                 funding_data = exchange.fetch_funding_rate_history(pair, since=since, limit=100)
#                 if not funding_data:
#                     break
                
#                 for entry in funding_data:
#                     standardized_entry = {
#                         'timestamp_ms': entry['timestamp'],
#                         'funding_rate': entry['fundingRate'],
#                         'datetime': entry['datetime']
#                     }
#                     data.append(standardized_entry)
                
#                 since = funding_data[-1]['timestamp'] + 1
#                 total_records += len(funding_data)
#                 time.sleep(exchange.rateLimit / 1000)
                
#             except Exception as e:
#                 error_msg = str(e).lower()
                
#                 # Controlla se è un errore di timeframe (se applicabile)
#                 if 'timeframe' in error_msg or 'interval' in error_msg:
#                     logger.error(f"❌ TIMEFRAME NON SUPPORTATO per funding su {exchange.id}: {funding_timeframe}")
#                     logger.info(f"💡 Prova a modificare FUNDING_TIMEFRAME in config.py")
#                     break
#                 elif 'not supported' in error_msg:
#                     logger.warning(f"⚠️ Funding rate non supportato per {exchange.id}")
#                     break
#                 elif 'end of data' in error_msg or 'no data' in error_msg:
#                     break
#                 else:
#                     logger.warning(f"⚠️ Errore funding API: {e}")
#                     time.sleep(5)
        
#         if data:
#             df = pd.DataFrame(data)
#             save_parquet(df, filepath, append=append)
#             logger.info(f"✅ Funding rate salvati: {len(df)} record (TF: {funding_timeframe})")
#             return df
#         else:
#             logger.info("ℹ️ Nessun dato funding disponibile")
            
#     except Exception as e:
#         logger.error(f"❌ Errore scaricamento funding: {e}")
    
#     return None

# def download_oi_data(exchange, pair, filepath, start_timestamp=None, append=False):
#     """Scarica dati di open interest per perpetual."""
    
#     oi_timeframe = OI_TIMEFRAME  # Usa il timeframe dal config
    
#     logger.info(f"📥 Scaricando open interest per {pair} (TF: {oi_timeframe})...")
    
#     data = []
#     since = start_timestamp if start_timestamp else get_oldest_timestamp(exchange, pair, 'perpetual')
#     latest_timestamp = get_current_timestamp_ms()

#     if append and os.path.exists(filepath):
#         existing_df = load_parquet(filepath)
#         if not existing_df.empty:
#             last_timestamp = existing_df['timestamp_ms'].max()
#             since = last_timestamp + 1
#             logger.info(f"🔄 Funding: continuando da {timestamp_to_datetime(last_timestamp)}")
#         else:
#             since = start_timestamp if start_timestamp else get_oldest_timestamp(exchange, pair, 'perpetual')
#     else:
#         since = start_timestamp if start_timestamp else get_oldest_timestamp(exchange, pair, 'perpetual')    
    
#     try:
#         total_records = 0
#         while since < latest_timestamp:
#             try:
#                 oi_data = exchange.fetch_open_interest_history(pair, oi_timeframe, since=since, limit=100)
#                 if not oi_data:
#                     break
                
#                 for entry in oi_data:
#                     standardized_entry = {
#                         'timestamp_ms': entry['timestamp'],
#                         'open_interest': entry['openInterest'],
#                         'datetime': entry['datetime']
#                     }
#                     data.append(standardized_entry)
                
#                 since = oi_data[-1]['timestamp'] + 1
#                 total_records += len(oi_data)
#                 time.sleep(exchange.rateLimit / 1000)
                
#             except Exception as e:
#                 error_msg = str(e).lower()
                
#                 # Controlla se è un errore di timeframe
#                 if 'timeframe' in error_msg or 'interval' in error_msg:
#                     logger.error(f"❌ TIMEFRAME NON SUPPORTATO per OI su {exchange.id}: {oi_timeframe}")
#                     logger.info(f"💡 Prova a modificare OI_TIMEFRAME in config.py (es. '5m', '15m', '1h')")
#                     break
#                 elif 'not supported' in error_msg:
#                     logger.warning(f"⚠️ Open Interest non supportato per {exchange.id}")
#                     break
#                 elif 'end of data' in error_msg or 'no data' in error_msg:
#                     break
#                 else:
#                     logger.warning(f"⚠️ Errore OI API: {e}")
#                     time.sleep(5)
        
#         if data:
#             df = pd.DataFrame(data)
#             save_parquet(df, filepath, append=append)
#             logger.info(f"✅ Open interest salvati: {len(df)} record (TF: {oi_timeframe})")
#             return df
#         else:
#             logger.info("ℹ️ Nessun dato open interest disponibile")
            
#     except Exception as e:
#         logger.error(f"❌ Errore scaricamento open interest: {e}")
    
#     return None

#def download_pair_data(exchange, pair_info, start_timestamp, append_mode, fetch_funding, fetch_oi):    # fetch_funding, fetch_oi non utilizzati
def download_pair_data(exchange, pair_info, start_timestamp, append_mode):
    """Scarica tutti i dati per una singola coppia."""
    pair = pair_info['symbol']
    market_type = pair_info['market_type']
    
    logger.info(f"\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
    logger.info(f"📊 Processing: {pair} ({market_type.upper()})")
    
    # Download candele OHLCV
    candles_filename = get_parquet_filename(exchange.id, pair, TIMEFRAME, market_type, 'candles')
    candles_path = f"{DATA_DIRECTORIES[market_type]}/{candles_filename}"
    
    df_candles = download_ohlcv_data(exchange, pair, market_type, candles_path, start_timestamp, append_mode)
    
    # Per perpetual, scarica metriche aggiuntive // Non usato per ora
    # if market_type == 'perpetual':
    #     if fetch_funding:
    #         funding_filename = get_parquet_filename(exchange.id, pair, TIMEFRAME, market_type, 'funding')
    #         funding_path = f"{DATA_DIRECTORIES['funding']}/{funding_filename}"
    #         download_funding_data(exchange, pair, funding_path, start_timestamp, append_mode)
        
    #     if fetch_oi:
    #         oi_filename = get_parquet_filename(exchange.id, pair, TIMEFRAME, market_type, 'oi')
    #         oi_path = f"{DATA_DIRECTORIES['open_interest']}/{oi_filename}"
    #         download_oi_data(exchange, pair, oi_path, start_timestamp, append_mode)
    
    return df_candles is not None

def main():
    global logger
    
    # Setup directories
    for directory in DATA_DIRECTORIES.values():
        ensure_directory_exists(directory)
    
    logger = setup_logger(LOGS_PATH)
    
    logger.info("🚀 Avvio MEHD - Multi-Exchanges Historical Data Downloader")
    logger.info("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
    
    try:
        # Input exchange
        exchange_name = input(f"{Fore.CYAN}Exchange ({', '.join(SUPPORTED_EXCHANGES)} [default: {DEFAULT_EXCHANGE}]): {Style.RESET_ALL}") or DEFAULT_EXCHANGE
        exchange = validate_exchange(exchange_name)
        
        # Input asset
        asset = input(f"{Fore.CYAN}Asset (es. BTC, ETH, SOL) [default: {DEFAULT_ASSET}]: {Style.RESET_ALL}") or DEFAULT_ASSET
        
        logger.info(f"🔍 Cercando coppie disponibili per {asset} su {exchange_name}...")
        available_pairs = get_available_pairs(exchange, asset)
        
        if not available_pairs:
            logger.error(f"❌ Nessuna coppia trovata per {asset} su {exchange_name}")
            return
        
        # Display available pairs
        logger.info(f"📊 COPPIE TROVATE PER {asset.upper()}:")
        for i, pair_info in enumerate(available_pairs, 1):
            logger.info(f"[{i}] {pair_info['symbol']} ({pair_info['market_type'].upper()})")
        
        logger.info(f"[{len(available_pairs) + 1}] Tutte le coppie - Scarica tutto in una volta")
        
        # User selection
        choice = input(f"{Fore.CYAN}Scelta [1-{len(available_pairs) + 1}]: {Style.RESET_ALL}")
        
        selected_pairs = []
        if choice == str(len(available_pairs) + 1):
            selected_pairs = available_pairs
            logger.info(f"✅ Selezionato: TUTTE LE COPPIE ({len(selected_pairs)} coppie)")
        else:
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(available_pairs):
                    selected_pairs = [available_pairs[idx]]
                    logger.info(f"✅ Selezionato: {selected_pairs[0]['symbol']}")
                else:
                    logger.error("❌ Scelta non valida")
                    return
            except ValueError:
                logger.error("❌ Input non valido")
                return
        
        # Additional metrics for perpetual // Non usato per ora
        # fetch_funding = FETCH_FUNDING
        # fetch_oi = FETCH_OPEN_INTEREST
        
        # perpetual_pairs = [p for p in selected_pairs if p['market_type'] == 'perpetual']
        # if perpetual_pairs:
        #     logger.info("💡 METRICHE AGGIUNTIVE (solo perpetual):")
        #     logger.info("[1] Funding Rate | [2] Open Interest | [3] Tutto")
        #     metrics_choice = input(f"{Fore.CYAN}Selezione [1,2,3]: {Style.RESET_ALL}")
            
        #     if metrics_choice == '1':
        #         fetch_oi = False
        #     elif metrics_choice == '2':
        #         fetch_funding = False
        #     elif metrics_choice == '3':
        #         pass  # Keep both True
        #     else:
        #         logger.info("✅ Metriche default: OHLCV + Funding + Open Interest")
        
        # Check existing files and choose mode
        logger.info("📁 CHECK FILES ESISTENTI:")
        existing_files = []
        for pair_info in selected_pairs:
            pair = pair_info['symbol']
            market_type = pair_info['market_type']
            
            candles_path = f"{DATA_DIRECTORIES[market_type]}/{get_parquet_filename(exchange.id, pair, TIMEFRAME, market_type, 'candles')}"
            if check_file_exists(candles_path):
                df = load_parquet(candles_path)
                candle_count = len(df) if not df.empty else 0
                existing_files.append((pair, candle_count))
                logger.info(f"• {pair}: ESISTE ({candle_count} candele)")
            else:
                logger.info(f"• {pair}: NON ESISTE")
        
        # Download mode
        logger.info("💡 MODALITÀ DOWNLOAD:")
        logger.info("(1) APPEND - Continua dai file esistenti, nuovi da zero")
        logger.info("(2) OVERWRITE - Ricomincia tutto da zero")
        
        mode_choice = input(f"{Fore.CYAN}Scelta [1-2]: {Style.RESET_ALL}")
        append_mode = mode_choice == '1'
        
        if append_mode:
        # APPEND: usa l'ultimo timestamp dai file esistenti
            logger.info("✅ Modalità: APPEND selezionata")
            logger.info("• File esistenti: continuano dall'ultima candela")
            logger.info("• File nuovi: partono dalla data di default")
            
            # Non chiedere start date per append - usa automaticamente l'ultimo timestamp
            start_timestamp = None  # Verrà determinato per ogni file nella download_pair_data
            
        else:
            # OVERWRITE: chiedi start date
            start_date_str = input(f"{Fore.CYAN}📅 Start date (YYYY-MM-DD) [default: {DEFAULT_START_DATE}]: {Style.RESET_ALL}") or DEFAULT_START_DATE
            start_timestamp = parse_date(start_date_str)
            
            logger.info(f"✅ Configurazione confermata:")
            logger.info(f"• Data inizio: {timestamp_to_datetime(start_timestamp)}")
            logger.info(f"• Timeframe: {TIMEFRAME}")
            logger.info(f"• File esistenti: sovrascritti")
            logger.info(f"• File nuovi: partono dal {timestamp_to_datetime(start_timestamp)}")
        
        # Start download
        logger.info("🚦 AVVIO DOWNLOAD...")
        
        success_count = 0
        for pair_info in selected_pairs:
            #success = download_pair_data(exchange, pair_info, start_timestamp, append_mode, fetch_funding, fetch_oi)
            success = download_pair_data(exchange, pair_info, start_timestamp, append_mode)
            if success:
                success_count += 1
        
        # Final summary
        logger.info("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
        if success_count == len(selected_pairs):
            logger.info("🎉 DOWNLOAD COMPLETATO CON SUCCESSO!")
        else:
            logger.warning(f"⚠️ DOWNLOAD PARZIALMENTE COMPLETATO: {success_count}/{len(selected_pairs)} coppie")
        
        logger.info(f"📊 Coppie processate: {success_count}/{len(selected_pairs)}")
        
    except KeyboardInterrupt:
        logger.info("\n⏹️ Download interrotto dall'utente")
    except Exception as e:
        logger.error(f"❌ Errore durante l'esecuzione: {e}")

if __name__ == "__main__":
    main()