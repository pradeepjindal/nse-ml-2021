	--select distinct symbol company,
	select symbol company,
		pivot_mtp,
		cmatp actual_atp,
		round(cmatp/pivot_mtp_one_pct,0) actual_atp_pct_spike,
		--round(cmatp/mtp_one_pct * mtp_one_pct,2) accurate_price_label,
		--round(round(cmatp/mtp_one_pct,3) * mtp_one_pct,0) precise_price_label,
		--round(round(cmatp/mtp_one_pct,1) * mtp_one_pct,0) refine_price_label,
		round(round(cmatp/pivot_mtp_one_pct,0) * pivot_mtp_one_pct,0) gross_price_label,
		'-----|-----' wall,
		round(pivot_mdq_k/100,2) pivot_mdq_Lakh,
		round(del_qty_k/100,2) actual_del_Lakh,
		round(delivered_qty/pivot_mdq_one_pct,0) actual_del_pct_spike
	from
	(
		select symbol, trade_date, cmatp, delivered_qty, round(delivered_qty/1000,0) del_qty_k
		from cfd_data_cd_left_join_f_mv
	 	where trade_date between to_date('2021-07-01', 'yyyy-MM-dd') and to_date('2021-09-01', 'yyyy-MM-dd')
	 			--AND symbol = 'COLPAL'
	) all_rows,
	(
		select symbol pivot_scrip, trade_date pivot_trade_date,
			round(cmatp,0) pivot_mtp,
			round(cmatp/100,2) pivot_mtp_one_pct,
			delivered_qty pivot_mdq,
			round(delivered_qty/100,0) pivot_mdq_one_pct,
			round(delivered_qty/1000,0) pivot_mdq_k,
			round((delivered_qty/1000)/100,0) pivot_mdq_k_one_pct
		from
			(
				select symbol, trade_date, cmatp, delivered_qty
				from cfd_data_cd_left_join_f_mv
				where trade_date between to_date('2021-07-01', 'yyyy-MM-dd') and to_date('2021-09-01', 'yyyy-MM-dd')
				--and symbol = 'COLPAL'
			) all_rows,
			(
				select symbol pivot_symbol, max(delivered_qty) pivot_del_qty
				from cfd_data_cd_left_join_f_mv
				where trade_date between to_date('2021-07-01', 'yyyy-MM-dd') and to_date('2021-09-01', 'yyyy-MM-dd')
				group by symbol
			) max_del_qty_tab
		where all_rows.symbol = max_del_qty_tab.pivot_symbol and all_rows.delivered_qty = max_del_qty_tab.pivot_del_qty
	) max_delivery_pivot_row
	where all_rows.symbol = max_delivery_pivot_row.pivot_scrip --order by mdq_pct
	--order by actual_del_qty_Lakh desc




-- --select
-- -- 	symbol, trade_date,
-- -- 	round(cmatp,0) mtp,
-- -- 	round(cmatp/100,2) mtp_one_pct,
-- -- 	delivered_qty mdq,
-- -- 	round(delivered_qty/100,0) mdq_one_pct,
-- -- 	round(delivered_qty/1000,0) mdq_k,
-- -- 	round((delivered_qty/1000)/100,0) mdq_k_one_pct
-- 		select symbol pivot_scrip, trade_date pivot_trade_date,
-- 		round(cmatp,0) pivot_mtp,
-- 		round(cmatp/100,2) pivot_mtp_one_pct,
-- 		delivered_qty pivot_mdq,
-- 		round(delivered_qty/100,0) pivot_mdq_one_pct,
-- 		round(delivered_qty/1000,0) pivot_mdq_k,
-- 		round((delivered_qty/1000)/100,0) pivot_mdq_k_one_pct
-- from
-- 	(
-- 		select symbol, trade_date, cmatp, delivered_qty
-- 		from cfd_data_cd_left_join_f_mv
-- 		where trade_date between to_date('2021-07-01', 'yyyy-MM-dd') and to_date('2021-09-01', 'yyyy-MM-dd')
-- 		and symbol = 'COLPAL'
-- 		order by trade_Date
-- 	) all_rows,
-- 	(
-- 		select symbol pivot_symbol, max(delivered_qty) pivot_del_qty
-- 		from cfd_data_cd_left_join_f_mv
-- 		where trade_date between to_date('2021-07-01', 'yyyy-MM-dd') and to_date('2021-09-01', 'yyyy-MM-dd')
-- 		group by symbol
-- 	) max_del_qty_tab
-- where all_rows.symbol = max_del_qty_tab.pivot_symbol and all_rows.delivered_qty = max_del_qty_tab.pivot_del_qty