select tdy_symbol symbol, tdy_trade_date trade_date, tdy_ha_open, tdy_ha_high, yes_ha_open, yes_ha_high
from
(
	select tdy_symbol, tdy_trade_date
	, tdy_ha_open, greatest(t0_high, tdy_ha_open, tdy_ha_close) tdy_ha_high, least(t0_low, tdy_ha_open, tdy_ha_close) tdy_ha_low, tdy_ha_close
	, yes_symbol, yes_trade_date trade_date
	, yes_ha_open, greatest(t1_high, yes_ha_open, yes_ha_close) yes_ha_high, least(t1_low, yes_ha_open, yes_ha_close) yes_ha_low, yes_ha_close
	from
	(
		SELECT
			t0_symbol tdy_symbol, t0_trade_date tdy_trade_date,
			round((t1_open + t1_close) /2, 0) tdy_ha_open,
			t0_high,
			t0_low,
			round((t0_open + t0_high + t0_low + t0_close) /4, 2) tdy_ha_close,
			'|' wall,
			t1_symbol yes_symbol, t1_trade_date yes_trade_date,
			round((t2_open + t2_close) /2, 0) yes_ha_open,
			t1_high,
			t1_low,
			round((t1_open + t1_high + t1_low + t1_close) /4, 2) yes_ha_close
		FROM
		(
			select symbol t0_symbol, trade_date t0_trade_date, open t0_open, high t0_high, low t0_low, close t0_close
			from nse_cash_market_tab
			where tdn = 20211122
		) t_zero,
		(
			select symbol t1_symbol, trade_date t1_trade_date, open t1_open, high t1_high, low t1_low, close t1_close
			from nse_cash_market_tab
			where tdn = (
				select max(tdn) yes_date from nse_cash_market_tab
				where tdn < (select max(tdn) from nse_cash_market_tab)
				--where tdn < 20211122
			)
		) t_minus_one,
		(
			select symbol t2_symbol, trade_date t2_trade_date, open t2_open, high t2_high, low t2_low, close t2_close from nse_cash_market_tab
			where tdn = (
				select max(tdn) yes_date from nse_cash_market_tab
				where tdn < (select max(tdn) yes_date from nse_cash_market_tab
							where tdn < (select max(tdn) from nse_cash_market_tab)
							)
				--where tdn < 20211122
			)
		) t_minus_two
		WHERE t_zero.t0_symbol = t_minus_one.t1_symbol and t_zero.t0_symbol = t_minus_two.t2_symbol
	) main
) grand, fno_symbol_mv1
--where tdy_ha_open = tdy_ha_high
--and tdy_symbol = 'CEBBCO'
where grand.tdy_symbol = fno_symbol_mv1.symbol
and tdy_ha_open = tdy_ha_high and yes_ha_open = yes_ha_high
order by tdy_symbol
