from src.option.cash_market_lib import get_close_prices
from src.option.future_lots_lib import get_future_data


# def main():
#     symbol_to_lot_map = get_future_data()
#     print(symbol_to_lot_map)
from src.option.max_pain_lib import get_max_pain

symbol_to_close = get_close_prices()

symbol_to_lot_map = get_future_data()
# print(symbol_to_lot_map)

symbol_to_pain_map = {}
csv_hdr = 'symbol,cm_close,max_pain,support,resist,call_close,range_pct,prem_pct,dist_pct'
print('{:16s} | {:8s} | {:8s} | {:8s} | {:8s} | {:8s} | {:8s}| {:8s} | {:8s} |'.format('symbol', 'cm_close', '  pain', ' support', 'resist', 'op_close', 'range_pct', ' pre_pct', 'dist_pct'))
for each_symbol in symbol_to_lot_map:
    max_pain, support, putt_close, resistance, call_close = get_max_pain(each_symbol, symbol_to_lot_map[each_symbol])
    # print(each_symbol, max_pain, support, resistance)
    if each_symbol in symbol_to_close:
        close_price = float(symbol_to_close[each_symbol])
    else:
        close_price = 0
    if close_price != 0:
        pct = call_close/ (close_price/100)
    else:
        pct = 0
    dist = 100 - close_price / (resistance/100)
    range = resistance - support
    mid = (resistance + support) / 2
    range_pct = range / (mid / 100)
    print('{:16s} | {:8.2f} | {:8.2f} | {:8.2f} | {:8.2f} | {:8.2f} | {:8.2f} | {:8.2f} | {:8.2f} |'.format(each_symbol, close_price, max_pain, support, resistance, call_close, range_pct, pct, dist))
    # csv_str = each_symbol + ',' + str(close_price) + ',' + str(max_pain) + ',' + str(support) + ',' + str(resistance) + ',' + str(call_close) + ',' + str(pct) + ',' + str(dist)
    # print(csv_str)
    symbol_to_pain_map[each_symbol] = max_pain

# print(symbol_to_pain_map)
