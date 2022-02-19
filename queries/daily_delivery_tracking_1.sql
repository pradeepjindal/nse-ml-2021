 SELECT dma.t1_td,
    dma.symbol,
    round(round(dma.t1_del::numeric / dma.t1_adq, 2) * 100::numeric, 0) AS spike1,
    round(round(dmb.t2_del::numeric / dmb.t2_adq, 2) * 100::numeric, 0) AS spike2,
    round(round(dmc.t3_del::numeric / dmc.t3_adq, 2) * 100::numeric, 0) AS spike3
   FROM
   (
SELECT dm.symbol, dm.trade_date AS t1_td, dm.deliverable_qty AS t1_del, mdv.adq AS t1_adq
   FROM nse_delivery_market_tab dm,
    z_fu_td_ranking_view1 tdr,
    ( SELECT lst.symbol, lst.trade_date, min(lst.expiry_date) AS min_expiry_date
      FROM nse_fo_tab lst
      GROUP BY lst.symbol, lst.trade_date) lsv,
    monthly_del_avg_view2 mdv
  WHERE dm.trade_date = tdr.trade_date AND tdr.trade_date_rank = 1 AND dm.symbol::text = lsv.symbol::text
  AND dm.trade_date = lsv.trade_date AND dm.symbol::text = mdv.symbol::text
  and date_trunc('month', lsv.min_expiry_date):: date = mdv.enm
) dma,
(
SELECT dm.symbol, dm.trade_date AS t2_td, dm.deliverable_qty AS t2_del, mdv.adq AS t2_adq
   FROM nse_delivery_market_tab dm,
    z_fu_td_ranking_view1 tdr,
    ( SELECT lst.symbol, lst.trade_date, min(lst.expiry_date) AS min_expiry_date
      FROM nse_fo_tab lst
      GROUP BY lst.symbol, lst.trade_date) lsv,
    monthly_del_avg_view2 mdv
  WHERE dm.trade_date = tdr.trade_date AND tdr.trade_date_rank = 2 AND dm.symbol::text = lsv.symbol::text
  AND dm.trade_date = lsv.trade_date AND dm.symbol::text = mdv.symbol::text
  and date_trunc('month', lsv.min_expiry_date):: date = mdv.enm
) dmb,
(
SELECT dm.symbol, dm.trade_date AS t3_td, dm.deliverable_qty AS t3_del, mdv.adq AS t3_adq
   FROM nse_delivery_market_tab dm,
    z_fu_td_ranking_view1 tdr,
    ( SELECT lst.symbol, lst.trade_date, min(lst.expiry_date) AS min_expiry_date
      FROM nse_fo_tab lst
      GROUP BY lst.symbol, lst.trade_date) lsv,
    monthly_del_avg_view2 mdv
  WHERE dm.trade_date = tdr.trade_date AND tdr.trade_date_rank = 3 AND dm.symbol::text = lsv.symbol::text
  AND dm.trade_date = lsv.trade_date AND dm.symbol::text = mdv.symbol::text
  and date_trunc('month', lsv.min_expiry_date):: date = mdv.enm
) dmc
  WHERE dma.symbol::text = dmb.symbol::text AND dmb.symbol::text = dmc.symbol::text
  ORDER BY (round(round(dma.t1_del::numeric / dma.t1_adq, 2) * 100::numeric, 0)) DESC;
