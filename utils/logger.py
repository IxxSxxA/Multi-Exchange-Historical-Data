# utils/logger.py
import logging
import datetime
import os
from colorama import init, Fore, Style

# Inizializza colorama (necessario per Windows)
init(autoreset=True)

def setup_logger(logs_path):
    """Setup del logger con output colorato."""
    log_filename = datetime.datetime.now().strftime('%Y-%m-%d.log')
    log_path = os.path.join(logs_path, log_filename)
    
    logger = logging.getLogger('MEHD')
    logger.setLevel(logging.INFO)
    
    # Formattatore personalizzato con colori
    class ColoredFormatter(logging.Formatter):
        FORMATS = {
            logging.INFO: f"{Fore.GREEN}%(asctime)s - %(levelname)s - %(message)s{Style.RESET_ALL}",
            logging.WARNING: f"{Fore.YELLOW}%(asctime)s - %(levelname)s - %(message)s{Style.RESET_ALL}",
            logging.ERROR: f"{Fore.RED}%(asctime)s - %(levelname)s - %(message)s{Style.RESET_ALL}"
        }

        def format(self, record):
            formatter = logging.Formatter(self.FORMATS.get(record.levelno, '%(asctime)s - %(levelname)s - %(message)s'))
            return formatter.format(record)

    # File handler (senza colori, per mantenere il file di log pulito)
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # Console handler (con colori)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(ColoredFormatter())

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger