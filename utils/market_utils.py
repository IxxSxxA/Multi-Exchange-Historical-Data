# utils/market_utils.py

def detect_market_type(exchange, pair):
    """Rileva se il pair è spot o perpetual."""
    market = exchange.markets[pair]
    
    if market.get('future', False) or market.get('linear', False) or market.get('inverse', False):
        return 'perpetual'
    elif market.get('spot', False):
        return 'spot'
    else:
        # Fallback: analizza il simbolo
        if '/USDT' in pair or '/BUSD' in pair:
            if 'SWAP' in pair or 'PERP' in pair or '-USDT' in pair.replace('/', ''):
                return 'perpetual'
        return 'spot'

def get_perpetual_symbol(exchange, pair):
    """Restituisce il simbolo corretto per perpetual."""
    market = exchange.markets[pair]
    if market.get('future', False):
        return market['id']
    # Per exchange come Bybit che usano la convenzione BTC/USDT:USDT
    return f"{pair}:USDT" if ':USDT' not in pair else pair