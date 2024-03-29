import datetime
import xlwings as xw
import psycopg2

call_sql ="""
    select symbol, trade_date, expiry_date, option_type, strike_price strike, close, open_int/2800 oi, change_in_oi/2800 oic
    from nse_option_market_tab 
    where symbol = 'TATAMOTORS' and option_type = 'CE' and trade_Date = to_Date('2021-10-08','yyyy-MM-dd')  and expiry_Date = to_Date('2021-10-28','yyyy-MM-dd')
    order by strike_price
"""
putt_sql ="""
    select symbol, trade_date, expiry_date, option_type, strike_price strike, close, open_int/2800 oi, change_in_oi/2800 oic
    from nse_option_market_tab 
    where symbol = 'TATAMOTORS' and option_type = 'PE' and trade_Date = to_Date('2021-10-08','yyyy-MM-dd')  and expiry_Date = to_Date('2021-10-28','yyyy-MM-dd')
    order by strike_price
"""
connection = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="localhost", port=5433)
cursor = connection.cursor()
cursor.execute(call_sql)

print(datetime.datetime.now())
call_records = cursor.fetchall()
print(datetime.datetime.now())

print('symbol, trade_date, expiry_date, option_type, strike_price strike, close,  oi,oic')
for each_record in call_records:
    print(each_record)

cursor.execute(putt_sql)

print(datetime.datetime.now())
putt_records = cursor.fetchall()
print(datetime.datetime.now())

print('symbol, trade_date, expiry_date, option_type, strike_price strike, close,  oi,oic')
for each_record in putt_records:
    print(each_record)

# print("Data from Database:- ", records)
connection.close()
# ================================================================================================
strike_map = {}
strike_array = []

for each_record in call_records:
    strike_map[each_record[4]] = each_record[4]

for each_record in putt_records:
    strike_map[each_record[4]] = each_record[4]

for each_key in strike_map.keys():
    strike_array.append(each_key)


call_vertical_sum_map = {}
putt_vertical_sum_map = {}
for each_strike in strike_array:
    call_vertical_sum_map[each_strike] = 0
    putt_vertical_sum_map[each_strike] = 0

# --------------------------------------------------------------------------------------------------
lot_size = 2800
call_strike_to_oi_map = {}

for each_record in call_records:
    strike = each_record[4]
    oi = each_record[6]
    call_strike_to_oi_map[strike] = oi

for vertical_strike in strike_array:
    if vertical_strike in call_strike_to_oi_map:
        oi = call_strike_to_oi_map[vertical_strike]
    else:
        oi = 0
    for horizontal_strike in strike_array:
        # calc is reverse or pe
        intrinsic_value = horizontal_strike - vertical_strike
        if intrinsic_value > 0:
            calculated_value = intrinsic_value * oi * lot_size
        else:
            calculated_value = 0
        call_vertical_sum_map[horizontal_strike] = call_vertical_sum_map[horizontal_strike] + calculated_value

# ---------------------------------------------------------------------------------------

lot_size = 2800
putt_strike_to_oi_map = {}

for each_record in putt_records:
    strike = each_record[4]
    oi = each_record[6]
    putt_strike_to_oi_map[strike] = oi

for vertical_strike in strike_array:
    if vertical_strike in putt_strike_to_oi_map:
        oi = putt_strike_to_oi_map[vertical_strike]
    else:
        oi = 0
    for horizontal_strike in strike_array:
        # calc is reverse or cal
        intrinsic_value = vertical_strike - horizontal_strike
        if intrinsic_value > 0:
            calculated_value = intrinsic_value * oi * lot_size
        else:
            calculated_value = 0
        putt_vertical_sum_map[horizontal_strike] = putt_vertical_sum_map[horizontal_strike] + calculated_value

# ---------------------------------------------------------------------------------------
putt_cal_array = []
putt_cal_map = {}
for each_strike in strike_array:
    if each_strike in call_vertical_sum_map:
        cal_sum = call_vertical_sum_map[each_strike]
    else:
        cal_sum = 0
    if each_strike in putt_vertical_sum_map:
        putt_sum = putt_vertical_sum_map[each_strike]
    else:
        putt_sum = 0
    putt_cal = cal_sum + putt_sum
    putt_cal_map[each_strike] = putt_cal
    if putt_cal > 0:
        putt_cal_array.append(putt_cal)

putt_cal_array.sort()
print(putt_cal_array[0])
m = putt_cal_array[0]
for each_strike in putt_cal_map.keys():
    if putt_cal_map[each_strike] == m:
        print(each_strike)


# =====================================================================
# wb = xw.Book() # wb = xw.Book(filename) would open an existing file
#
# # creates a worksheet object assigns it to ws
# ws1 = wb.sheets["Sheet1"]
# ws1.name = "ce-max-pain"
#
# #ws.range("A1") is a Range object
# ws1.range("C1").value = strike_array
# array_idx = 0
# for each_strike in strike_array:
#     cell_value = strike_array[array_idx]
#     array_idx = array_idx + 1
#     cell_idx = array_idx + 3
#     cell_name = "A" + str(cell_idx)
#     ws1.range(cell_name).value = cell_value
#     #
#     oi_cell_name = "B" + str(cell_idx)
#     if cell_value in ce_oi_map:
#         oi_value = ce_oi_map[cell_value]
#     else:
#         oi_value = 0
#     ws1.range(oi_cell_name).value = oi_value
#
#
# # ws.clear_contents()
# # ws.range("A1").options(index=False).value = strike_array
#
# wb.save('ce-max-pain-1.xlsx')
# # xw.apps[0].quit()
#
# # ---------------------------------------------------------------------------------------
# wb2 = xw.Book() # wb = xw.Book(filename) would open an existing file
#
# #creates a worksheet object assigns it to ws
# ws2 = wb2.sheets["Sheet1"]
# # ws2.name = "pe-max-pain"
#
# #ws.range("A1") is a Range object
# ws2.range("C1").value = strike_array
# array_idx = 0
# for each_strike in strike_array:
#     cell_value = strike_array[array_idx]
#     array_idx = array_idx + 1
#     cell_idx = array_idx + 3
#     cell_name = "A" + str(cell_idx)
#     ws2.range(cell_name).value = cell_value
#     #
#     oi_cell_name = "B" + str(cell_idx)
#     if cell_value in pe_oi_map:
#         oi_value = pe_oi_map[cell_value]
#     else:
#         oi_value = 0
#     ws2.range(oi_cell_name).value = oi_value
#
#
# # ws.clear_contents()
# # ws.range("A1").options(index=False).value = strike_array
#
# wb2.save('pe-max-pain-1.xlsx')
# xw.apps[0].quit()
