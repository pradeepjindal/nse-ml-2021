with t AS (
	select 'KOTAKBANK' s,
	--to_date('2021-01-01', 'yyyy-MM-dd') f, 	to_date('2021-3-30', 'yyyy-MM-dd') t
	--to_date('2021-04-01', 'yyyy-MM-dd') f, 	to_date('2021-6-30', 'yyyy-MM-dd') t
	--to_date('2021-04-01', 'yyyy-MM-dd') f, 	to_date('2021-8-30', 'yyyy-MM-dd') t
	to_date('2020-07-01', 'yyyy-MM-dd') f, 	to_date('2021-8-30', 'yyyy-MM-dd') t
)

SELECT
symbol, nominal_price_label, round((mdq_one_pct*sum_mdq_pct)/100000,0) mdq_pct_value_LAKH, atp_pct, sum_mdq_pct, mdq_pct_bar
FROM
(
	select symbol, atp_pct, sum(mdq_pct) sum_mdq_pct, rpad('='::text, (sum(mdq_pct)/10)::int, '='::text) mdq_pct_bar
	from
	(
		select symbol, trade_date, cmatp atp, round(cmatp/(mtp/100),0) atp_pct, round(delivered_qty/(mdq/100),0) mdq_pct
		from cfd_data_cd_left_join_f_mv mv,
		(
			select symbol scrip, trade_date td, round(cmatp,0) mtp, delivered_qty mdq from cfd_data_cd_left_join_f_mv
			where symbol = (select s from t) and trade_date between (select f from t) and (select t from t)
			and delivered_qty = (
			select max(delivered_qty) max_delivered_qty from cfd_data_cd_left_join_f_mv
				where symbol = (select s from t) and trade_date between (select f from t) and (select t from t)
			)
		) tab
		where mv.symbol = tab.scrip and trade_date between (select f from t) and (select t from t)
	) tabbie
	group by symbol, atp_pct
) abc,
(
	select distinct symbol company, round(cmatp/(mtp/100),0) company_atp_pct,
		round(round(cmatp/(mtp/100),0) * (mtp/100) ,0) nominal_price_label,
		round(mdq/100,0) mdq_one_pct
	from cfd_data_cd_left_join_f_mv mv,
	(
		select symbol scrip, trade_date td, round(cmatp,0) mtp, delivered_qty mdq from cfd_data_cd_left_join_f_mv
		where symbol = (select s from t) and trade_date between (select f from t) and (select t from t)
		and delivered_qty = (
		select max(delivered_qty) max_delivered_qty from cfd_data_cd_left_join_f_mv
			where symbol = (select s from t) and trade_date between (select f from t) and (select t from t)
		)
	) tab
	where mv.symbol = tab.scrip and trade_date between (select f from t) and (select t from t)
	--order by company_atp_pctv
) xyz
WHERE abc.symbol = xyz.company and abc.atp_pct = xyz.company_atp_pct
order by atp_pct desc
