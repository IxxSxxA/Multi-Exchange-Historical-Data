# utils/date_utils.py
from datetime import datetime, timezone
import time



def get_current_timestamp_ms():
    """Restituisce il timestamp corrente in millisecondi."""
    return int(time.time() * 1000)

def timestamp_to_datetime(timestamp_ms, tz=timezone.utc):
    """Converte timestamp_ms in datetime object."""

    # Stampa il timestamp per debug
    # print(f"Timestamp: {timestamp_ms}")
    
    # tz = timezone.utc if timezone == 'UTC' else None
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=tz)
    

def datetime_to_timestamp_ms(dt):
    """Converte datetime in timestamp_ms."""
    return int(dt.timestamp() * 1000)

def parse_date(date_str):
    """Parsa una stringa data in formato YYYY-MM-DD in timestamp_ms (inizio giornata)."""
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return datetime_to_timestamp_ms(dt)
    except ValueError:
        raise ValueError("Formato data non valido. Usa YYYY-MM-DD.")