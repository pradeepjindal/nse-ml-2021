SELECT ft.symbol, ft.trade_date, min(ft.expiry_date) AS min_expiry_date
FROM nse_future_market_tab ft
where symbol = 'COLPAL'
GROUP BY ft.symbol, ft.trade_date
