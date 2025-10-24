# utils/market_utils.py
import re

def detect_market_type(exchange, pair):
    """Rileva se il pair è spot o perpetual, escludendo futures e opzioni."""
    try:
        market = exchange.markets[pair]
        
        # 1. Controlla proprietà esplicite del market
        if market.get('spot', False):
            return 'spot'
        
        # 2. Escludi TUTTI i simboli che contengono trattini (futures/opzioni)
        symbol = market.get('symbol', '').upper()
        
        # Pattern semplice: se contiene "-", escludi // Per non considerare futures
        if re.search(r'-', symbol):
            return 'exclude'
        
        # 3. Se è un derivato senza trattini, è likely perpetual
        if (market.get('future', False) or 
            market.get('swap', False) or
            market.get('linear', False) or 
            market.get('inverse', False)):
            return 'perpetual'
            
        # 4. Fallback: analisi euristica del simbolo
        perpetual_indicators = [
            ':USDT', 'PERP', 'SWAP', 'PERPETUAL'
        ]
        
        for indicator in perpetual_indicators:
            if indicator in symbol:
                return 'perpetual'
        
        # 5. Se non possiamo classificare chiaramente, escludi per sicurezza
        return 'exclude'
        
    except Exception as e:
        return 'exclude'

def get_available_pairs(exchange, asset):
    """Trova tutti i pair disponibili per un asset, escludendo futures e opzioni."""
    available_pairs = []
    
    for symbol, market in exchange.markets.items():
        # Estrai la base currency (prima parte del simbolo)
        base_currency = symbol.split('/')[0].split(':')[0]
        
        # Filtra per asset e market attivo
        if (base_currency.upper() == asset.upper() and 
            market.get('active', False)):
            
            # Determina il tipo di mercato
            market_type = detect_market_type(exchange, symbol)
            
            # Includi solo spot e perpetual, escludi il resto
            if market_type in ['spot', 'perpetual']:
                available_pairs.append({
                    'symbol': symbol,
                    'market_type': market_type,
                    'quote_asset': symbol.split('/')[1] if '/' in symbol else 'UNKNOWN'
                })
    
    return available_pairs

def format_volume_display(volume):
    """Formatta il volume per display."""
    if volume >= 1_000_000:
        return f"{volume/1_000_000:.1f}M"
    elif volume >= 1_000:
        return f"{volume/1_000:.1f}K"
    else:
        return f"{volume:.0f}"