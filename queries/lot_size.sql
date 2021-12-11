select symbol, trade_month, expiry_date, min(open_int) lot_size
from
(
	select symbol, instrument, trade_Date, to_char(trade_Date,'yyyy-mm') trade_month, expiry_date, option_type
	, contracts, value_in_lakh, open_int, change_in_oi
	from nse_option_market_tab
	where change_in_oi > 0 and open_int=change_in_oi and open_int/change_in_oi=1
	order by trade_Date
) a
group by symbol, trade_month, expiry_date
order by symbol, trade_month, expiry_date
