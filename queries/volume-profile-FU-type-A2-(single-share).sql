SELECT
	symbol,
	nominal_price_label,
	round((mdq_one_pct*sum_mdq_pct)/100000,0) mdq_pct_value_LAKH,
	atp_pct_spike,
	sum_mdq_pct,
	round((mdq_one_pct*sum_mdq_pct)/del_gross_sum_one_pct,1) del_share,
	mdq_pct_bar,
	(mdq_one_pct*sum_mdq_pct) del_sum, del_gross_sum

FROM
(
	select symbol gross_symbol, sum(delivered_qty) del_gross_sum,
		round(sum(delivered_qty)/100,0) del_gross_sum_one_pct
	from cfd_data_cd_left_join_f_mv3
	where trade_date between to_date('2021-01-01', 'yyyy-MM-dd') and to_date('2021-12-30', 'yyyy-MM-dd')
	group by symbol
) del_gross_sum_at_symbol_levels,
(
		select symbol, atp_pct_spike, sum(mdq_pct_spike) sum_mdq_pct,
		rpad('='::text, (sum(mdq_pct_spike)/10)::int, '='::text) mdq_pct_bar
	from
	(
		select symbol, trade_date, cmatp atp, fuoi del_qty,
			round(cmatp/mtp_one_pct,0) atp_pct_spike,
			round(fuoi/mdq_one_pct,0) mdq_pct_spike
		from
		(
			select symbol, trade_date, cmatp, fuoi
			from cfd_data_cd_left_join_f_mv3
			where trade_date between to_date('2021-01-01', 'yyyy-MM-dd') and to_date('2021-12-30', 'yyyy-MM-dd')
		) all_rows,
		(
			select symbol scrip, trade_date td,
				round(cmatp,0) mtp, round(cmatp/100,0) mtp_one_pct,
				fuoi mdq, round(fuoi/100,0) mdq_one_pct
			from cfd_data_cd_left_join_f_mv3
			where symbol = 'GODREJCP' and trade_date between to_date('2021-01-01', 'yyyy-MM-dd') and to_date('2021-12-30', 'yyyy-MM-dd')
				and fuoi = (
					select max(fuoi) max_fuoi from cfd_data_cd_left_join_f_mv3
						where symbol = 'GODREJCP' and trade_date between to_date('2021-01-01', 'yyyy-MM-dd') and to_date('2021-12-30', 'yyyy-MM-dd')
					)
		) max_del_row_tab
		where all_rows.symbol = max_del_row_tab.scrip --and trade_date between (select f from t) and (select t from t)
	) tabbie
	group by symbol, atp_pct_spike
) del_sum_at_price_levels,
(
	select distinct symbol company,
		round(cmatp/(mtp/100),0) company_atp_pct,
		round(round(cmatp/(mtp/100),0) * (mtp/100) ,0) nominal_price_label,
		round(mdq/100,0) mdq_one_pct
	from
	(
		select symbol, trade_date, cmatp
	 	from cfd_data_cd_left_join_f_mv3
	 	where trade_date between to_date('2021-01-01', 'yyyy-MM-dd') and to_date('2021-12-30', 'yyyy-MM-dd')
	) all_rows,
	(
		select symbol scrip, trade_date td, round(cmatp,0) mtp, delivered_qty mdq
		from cfd_data_cd_left_join_f_mv3
		where symbol = 'GODREJCP' and trade_date between to_date('2021-01-01', 'yyyy-MM-dd') and to_date('2021-12-30', 'yyyy-MM-dd')
			and delivered_qty = (
				select max(delivered_qty) max_delivered_qty from cfd_data_cd_left_join_f_mv3
					where symbol = 'GODREJCP' and trade_date between to_date('2021-01-01', 'yyyy-MM-dd') and to_date('2021-12-30', 'yyyy-MM-dd')
				)
	) max_del_row_tab
	where all_rows.symbol = max_del_row_tab.scrip --and trade_date between (select f from t) and (select t from t)
	--order by company_atp_pctv
) price_levels
WHERE del_gross_sum_at_symbol_levels.gross_symbol = del_sum_at_price_levels.symbol
	and del_sum_at_price_levels.symbol = price_levels.company
	and del_sum_at_price_levels.atp_pct_spike = price_levels.company_atp_pct
order by atp_pct_spike desc
