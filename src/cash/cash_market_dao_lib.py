import psycopg2


def get_close_prices():
    cash_market_sql ="""
        select symbol, trade_date, close
        from nse_cash_market_tab 
        where trade_Date = to_Date('2021-10-12','yyyy-MM-dd')
        order by symbol
    """

    connection = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="localhost", port=5433)
    cursor = connection.cursor()
    cursor.execute(cash_market_sql)

    # print(datetime.datetime.now())
    cash_market_records = cursor.fetchall()
    # print(datetime.datetime.now())

    symbol_to_close = {}
    for each_record in cash_market_records:
        symbol = each_record[0]
        symbol_to_close[symbol] = each_record[2]

    return symbol_to_close
