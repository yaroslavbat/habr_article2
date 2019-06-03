--******************************************************************************
-- Indicator EMASIMPLE
--******************************************************************************

rollback;

delete HABR_MARKETINDEXES_PARMSEL_RESULTS where INDICATOR_NAME = 'HABR_MARKETINDEXES_EMASIMPLE_F_CALC';
commit;

insert into HABR_MARKETINDEXES_PARMSEL_RESULTS (INDICATOR_NAME, PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK)
with
  TP1 as (select rownum as PARM1 from dual connect by level <= &&AVERAGING_INTERVAL)
, TP2 as (select rownum as PARM2 from dual connect by level <= &&LAG_MODELLING_DEPTH)
select --+ parallel(8)
       'HABR_MARKETINDEXES_EMASIMPLE_F_CALC', PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK
from TP1
cross join TP2
cross join table (HABR_TRADEMODELLING_P.CALC_ACTIONS_TOTALS (cursor (select STOCK_NAME, ADATE, ACLOSE, AACTION from table (HABR_MARKETINDEXES_EMASIMPLE_F_CALC (PARM1))), PARM2))
;
commit;




--******************************************************************************
-- Indicator CROSSES
--******************************************************************************

rollback;

delete HABR_MARKETINDEXES_PARMSEL_RESULTS where INDICATOR_NAME = 'HABR_MARKETINDEXES_CROSSES_F_CALC';
commit;

insert into HABR_MARKETINDEXES_PARMSEL_RESULTS (INDICATOR_NAME, PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK)
with
  TP1 as (select rownum as PARM1 from dual connect by level <= &&AVERAGING_INTERVAL)
, TP2 as (select rownum as PARM2 from dual connect by level <= &&LAG_MODELLING_DEPTH)
select --+ parallel(8)
       'HABR_MARKETINDEXES_CROSSES_F_CALC', PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK
from TP1
cross join TP2
cross join table (HABR_TRADEMODELLING_P.CALC_ACTIONS_TOTALS (cursor (select STOCK_NAME, ADATE, ACLOSE, AACTION from table (HABR_MARKETINDEXES_CROSSES_F_CALC (PARM1))), PARM2)) a
;
commit;



--******************************************************************************
-- Indicator OBV, On-Balance Volume
--******************************************************************************

rollback;

delete HABR_MARKETINDEXES_PARMSEL_RESULTS where INDICATOR_NAME = 'HABR_MARKETINDEXES_OBV_F_CALC';
commit;

insert into HABR_MARKETINDEXES_PARMSEL_RESULTS (INDICATOR_NAME, PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK)
with
  TP1 as (select rownum as PARM1 from dual connect by level <= &&AVERAGING_INTERVAL)
, TP2 as (select rownum as PARM2 from dual connect by level <= &&LAG_MODELLING_DEPTH)
select --+ parallel(8)
       'HABR_MARKETINDEXES_OBV_F_CALC', PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK
from TP1
cross join TP2
cross join table (HABR_TRADEMODELLING_P.CALC_ACTIONS_TOTALS (cursor (select STOCK_NAME, ADATE, ACLOSE, AACTION from table (HABR_MARKETINDEXES_OBV_F_CALC (PARM1))), PARM2)) a
;
commit;




--******************************************************************************
-- Indicator KELTNER, Keltner Channel
--******************************************************************************

rollback;

delete HABR_MARKETINDEXES_PARMSEL_RESULTS where INDICATOR_NAME = 'HABR_MARKETINDEXES_KELTNER_F_CALC';
commit;

insert into HABR_MARKETINDEXES_PARMSEL_RESULTS (INDICATOR_NAME, PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK)
with
  TP1 as (select rownum as PARM1 from dual connect by level <= &&AVERAGING_INTERVAL)
, TP2 as (select rownum as PARM2 from dual connect by level <= &&LAG_MODELLING_DEPTH)
select --+ parallel(8)
       'HABR_MARKETINDEXES_KELTNER_F_CALC', PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK
from TP1
cross join TP2
cross join table (HABR_TRADEMODELLING_P.CALC_ACTIONS_TOTALS (cursor (select STOCK_NAME, ADATE, ACLOSE, AACTION from table (HABR_MARKETINDEXES_KELTNER_F_CALC (PARM1))), PARM2)) a
;
commit;



--******************************************************************************
-- Indicator PVT, Price-Volume Trend
--******************************************************************************

rollback;

delete HABR_MARKETINDEXES_PARMSEL_RESULTS where INDICATOR_NAME = 'HABR_MARKETINDEXES_PVT_F_CALC';
commit;

insert into HABR_MARKETINDEXES_PARMSEL_RESULTS (INDICATOR_NAME, PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK)
with
  TP1 as (select rownum as PARM1 from dual connect by level <= &&AVERAGING_INTERVAL)
, TP2 as (select rownum as PARM2 from dual connect by level <= &&LAG_MODELLING_DEPTH)
select --+ parallel(8) 
       'HABR_MARKETINDEXES_PVT_F_CALC', PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK
from TP1
cross join TP2
cross join table (HABR_TRADEMODELLING_P.CALC_ACTIONS_TOTALS (cursor (select STOCK_NAME, ADATE, ACLOSE, AACTION from table (HABR_MARKETINDEXES_PVT_F_CALC (PARM1))), PARM2)) a
;
commit;



--******************************************************************************
-- Indicator EMV, Arms’ Ease of Movement Value
--******************************************************************************

rollback;

delete HABR_MARKETINDEXES_PARMSEL_RESULTS where INDICATOR_NAME = 'HABR_MARKETINDEXES_EMV_F_CALC';
commit;

insert into HABR_MARKETINDEXES_PARMSEL_RESULTS (INDICATOR_NAME, PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK)
with
  TP1 as (select rownum as PARM1 from dual connect by level <= &&AVERAGING_INTERVAL)
, TP2 as (select rownum as PARM2 from dual connect by level <= &&LAG_MODELLING_DEPTH)
select --+ parallel(8)
       'HABR_MARKETINDEXES_EMV_F_CALC', PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK
from TP1
cross join TP2
cross join table (HABR_TRADEMODELLING_P.CALC_ACTIONS_TOTALS (cursor (select STOCK_NAME, ADATE, ACLOSE, AACTION from table (HABR_MARKETINDEXES_EMV_F_CALC (PARM1))), PARM2)) a
;
commit;


--******************************************************************************
-- Indicator CCI, Commodity Channel Index
--******************************************************************************

rollback;

delete HABR_MARKETINDEXES_PARMSEL_RESULTS where INDICATOR_NAME = 'HABR_MARKETINDEXES_CCI_F_CALC';
commit;

insert into HABR_MARKETINDEXES_PARMSEL_RESULTS (INDICATOR_NAME, PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK)
with
  TP1 as (select rownum as PARM1 from dual connect by level <= &&AVERAGING_INTERVAL)
, TP2 as (select rownum as PARM2 from dual connect by level <= &&LAG_MODELLING_DEPTH)
select --+ parallel(8)
       'HABR_MARKETINDEXES_CCI_F_CALC', PARM1, PARM2, STOCK_NAME, ADATE_MIN, ADATE_MAX, DEALS_COUNT, BALANCE_RESULT, DEALS_PROFIT_AMOUNT, DEALS_LOSS_AMOUNT, DEALS_PROFIT_COUNT, DEALS_LOSS_COUNT, IN_STOCK
from TP1
cross join TP2
cross join table (HABR_TRADEMODELLING_P.CALC_ACTIONS_TOTALS (cursor (select STOCK_NAME, ADATE, ACLOSE, AACTION from table (HABR_MARKETINDEXES_CCI_F_CALC (PARM1))), PARM2)) a
;
commit;


