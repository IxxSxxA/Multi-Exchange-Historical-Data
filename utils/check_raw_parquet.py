# utils/check_raw_parquet.py
# file indipendente
# python utils/check_raw_parquet.py

import os
import pandas as pd
import glob
from colorama import init, Fore, Style

init(autoreset=True)

def check_parquet_file(filepath):
    """Controlla un singolo file Parquet e restituisce le informazioni."""
    try:
        # Prova a leggere il file
        df = pd.read_parquet(filepath)
        
        # Informazioni base
        file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
        row_count = len(df)
        columns = list(df.columns)
        
        # Informazioni aggiuntive se disponibili
        date_range = ""
        if 'timestamp_ms' in df.columns:
            df['datetime'] = pd.to_datetime(df['timestamp_ms'], unit='ms')
            date_range = f"{df['datetime'].min()} to {df['datetime'].max()}"
        elif 'datetime' in df.columns:
            date_range = f"{df['datetime'].min()} to {df['datetime'].max()}"
        
        return {
            'status': 'OK',
            'file_size_mb': round(file_size, 2),
            'row_count': row_count,
            'columns': columns,
            'date_range': date_range,
            'error': None
        }
        
    except Exception as e:
        return {
            'status': 'ERROR',
            'file_size_mb': 0,
            'row_count': 0,
            'columns': [],
            'date_range': '',
            'error': str(e)
        }

def find_parquet_files(base_path='data'):
    """Trova tutti i file Parquet nelle sottocartelle di data/."""
    parquet_files = []
    
    if not os.path.exists(base_path):
        print(f"{Fore.RED}❌ La cartella {base_path} non esiste!{Style.RESET_ALL}")
        return parquet_files
    
    # Cerca ricorsivamente tutti i file .parquet
    pattern = os.path.join(base_path, '**', '*.parquet')
    parquet_files = glob.glob(pattern, recursive=True)
    
    return parquet_files

def display_file_info(filepath, info):
    """Mostra le informazioni di un file in formato leggibile."""
    filename = os.path.basename(filepath)
    relative_path = os.path.relpath(filepath, 'data')
    
    if info['status'] == 'OK':
        print(f"{Fore.GREEN}✅ {filename}{Style.RESET_ALL}")
        print(f"   📁 Percorso: {relative_path}")
        print(f"   📊 Dimensioni: {info['file_size_mb']} MB")
        print(f"   📈 Righe: {info['row_count']:,}")
        print(f"   🗂️  Colonne: {', '.join(info['columns'])}")
        if info['date_range']:
            print(f"   📅 Periodo: {info['date_range']}")
    else:
        print(f"{Fore.RED}❌ {filename}{Style.RESET_ALL}")
        print(f"   📁 Percorso: {relative_path}")
        print(f"   💥 Errore: {info['error']}")
    print()

def main():
    print(f"{Fore.CYAN}🔍 PARQUET FILE CHECKER{Style.RESET_ALL}")
    print("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
    
    # Trova tutti i file Parquet
    parquet_files = find_parquet_files()
    
    if not parquet_files:
        print(f"{Fore.YELLOW}ℹ️  Nessun file Parquet trovato nella cartella data/{Style.RESET_ALL}")
        return
    
    print(f"📁 Trovati {len(parquet_files)} file Parquet:")
    for i, filepath in enumerate(parquet_files, 1):
        filename = os.path.basename(filepath)
        relative_path = os.path.relpath(filepath, 'data')
        print(f"[{i}] {filename}")
        print(f"    {relative_path}")
    
    print(f"\n{Fore.CYAN}💡 OPZIONI DI CHECK:{Style.RESET_ALL}")
    print("[1-{n}] Check singolo file")
    print("[a]    Check tutti i file")
    print("[q]    Esci")
    
    choice = input(f"\n{Fore.CYAN}Scelta: {Style.RESET_ALL}").lower()
    
    files_to_check = []
    
    if choice == 'a':
        files_to_check = parquet_files
        print(f"\n{Fore.YELLOW}🔍 Check di TUTTI i {len(parquet_files)} file...{Style.RESET_ALL}")
    elif choice == 'q':
        print("Arrivederci! 👋")
        return
    elif choice.isdigit():
        idx = int(choice) - 1
        if 0 <= idx < len(parquet_files):
            files_to_check = [parquet_files[idx]]
            print(f"\n{Fore.YELLOW}🔍 Check del file selezionato...{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}❌ Scelta non valida!{Style.RESET_ALL}")
            return
    else:
        print(f"{Fore.RED}❌ Input non riconosciuto!{Style.RESET_ALL}")
        return
    
    # Esegui il check
    print("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
    
    ok_count = 0
    error_count = 0
    total_rows = 0
    total_size = 0
    
    for filepath in files_to_check:
        info = check_parquet_file(filepath)
        display_file_info(filepath, info)
        
        if info['status'] == 'OK':
            ok_count += 1
            total_rows += info['row_count']
            total_size += info['file_size_mb']
        else:
            error_count += 1
    
    # Riepilogo finale
    print("▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬")
    print(f"{Fore.CYAN}📊 RIEPILOGO CHECK:{Style.RESET_ALL}")
    print(f"✅ File OK: {ok_count}")
    print(f"❌ File con errori: {error_count}")
    
    if ok_count > 0:
        print(f"📈 Righe totali: {total_rows:,}")
        print(f"💾 Dimensione totale: {total_size:.2f} MB")
        print(f"📊 Media righe/file: {total_rows // ok_count if ok_count > 0 else 0:,}")
    
    if error_count == 0 and ok_count > 0:
        print(f"\n{Fore.GREEN}🎉 Tutti i file sono integri e leggibili!{Style.RESET_ALL}")
    elif error_count > 0:
        print(f"\n{Fore.YELLOW}⚠️  Alcuni file hanno problemi di lettura{Style.RESET_ALL}")

if __name__ == "__main__":
    main()