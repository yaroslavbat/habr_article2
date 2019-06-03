-- This is not a script, but set of statements
-- Run statements One-by-One

--******************************************************************************
-- Indicator EMASIMPLE
--******************************************************************************


select COLUMN_VALUE as ALG, dbms_sqlhash.gethash (COLUMN_VALUE, 2) as RECORDSET_HASH
from table (sys.odcivarchar2list ('select * from table (HABR_MARKETINDEXES_EMASIMPLE_F_CALC (15)) order by 1, 2'
                                , 'select * from table (HABR_MARKETINDEXES_EMASIMPLE_F_RECU (15)) order by 1, 2'
                                , 'select * from table (HABR_MARKETINDEXES_EMASIMPLE_F_MODE (15)) order by 1, 2'
                                , 'select * from table (HABR_MARKETINDEXES_EMASIMPLE_F_AGRF (15)) order by 1, 2'));


select coalesce (a.STOCK_NAME, b.STOCK_NAME) as STOCK_NAME, coalesce (a.ADATE, b.ADATE) as ADATE
     , a.ACLOSE as CALC_ACLOSE, b.ACLOSE as AGRF_CLOSE
     , a.IND_VALUE as CALC_EMA, b.IND_VALUE as AGRF_EMA
     , a.AACTION as CALC_AACTION, b.AACTION as AGRF_AACTION
from table (HABR_MARKETINDEXES_EMASIMPLE_F_CALC (15)) a
full outer join table (HABR_MARKETINDEXES_EMASIMPLE_F_AGRF (15)) b on a.STOCK_NAME = b.STOCK_NAME and a.ADATE = b.ADATE
--where sys_op_map_nonnull (a.ACLOSE) <> sys_op_map_nonnull (b.ACLOSE)
--   or sys_op_map_nonnull (a.IND_VALUE) <> sys_op_map_nonnull (b.IND_VALUE)
--   or sys_op_map_nonnull (a.AACTION) <> sys_op_map_nonnull (b.AACTION)
order by 1, 2;
;


--******************************************************************************
-- Indicator OBV, On-Balance Volume
--******************************************************************************

select COLUMN_VALUE as ALG, dbms_sqlhash.gethash (COLUMN_VALUE, 2) as RECORDSET_HASH
from table (sys.odcivarchar2list ('select * from table (HABR_MARKETINDEXES_OBV_F_CALC (5)) order by 1, 2'
                                , 'select * from table (HABR_MARKETINDEXES_OBV_F_SIMP (5)) order by 1, 2'
                                , 'select * from table (HABR_MARKETINDEXES_OBV_F_MODE (5)) order by 1, 2'));



select coalesce (a.STOCK_NAME, b.STOCK_NAME) as STOCK_NAME, coalesce (a.ADATE, b.ADATE) as ADATE
     , a.ACLOSE as CALC_ACLOSE, b.ACLOSE as SIMP_CLOSE
     , a.IND_VALUE as CALC_OBV, b.IND_VALUE as SIMP_OBV
     , a.IND_VALUE2 as CALC_OBV_AVG, b.IND_VALUE2 as SIMP_OBV_AVG
     , a.AACTION as CALC_AACTION, b.AACTION as SIMP_AACTION
from            table (HABR_MARKETINDEXES_OBV_F_CALC (5)) a
full outer join table (HABR_MARKETINDEXES_OBV_F_SIMP (5)) b on a.STOCK_NAME = b.STOCK_NAME and a.ADATE = b.ADATE
where sys_op_map_nonnull (a.ACLOSE) <> sys_op_map_nonnull (b.ACLOSE)
   or sys_op_map_nonnull (a.IND_VALUE) <> sys_op_map_nonnull (b.IND_VALUE)
   or sys_op_map_nonnull (a.IND_VALUE2) <> sys_op_map_nonnull (b.IND_VALUE2)
   or sys_op_map_nonnull (a.AACTION) <> sys_op_map_nonnull (b.AACTION)
order by 1, 2
;


--******************************************************************************
-- Indicator KELTNER, Keltner Channel
--******************************************************************************

select COLUMN_VALUE as ALG, dbms_sqlhash.gethash (COLUMN_VALUE, 2) as RECORDSET_HASH
from table (sys.odcivarchar2list ('select * from table (HABR_MARKETINDEXES_KELTNER_F_CALC (15)) order by 1, 2'
                                , 'select * from table (HABR_MARKETINDEXES_KELTNER_F_SIMP (15)) order by 1, 2'
                                , 'select * from table (HABR_MARKETINDEXES_KELTNER_F_MODE (15)) order by 1, 2'));


select coalesce (a.STOCK_NAME, b.STOCK_NAME) as STOCK_NAME, coalesce (a.ADATE, b.ADATE) as ADATE
     , a.ACLOSE as CALC_ACLOSE, b.ACLOSE as SIMP_ACLOSE
     , a.IND_VALUE as CALC_KELTNER_LOW, b.IND_VALUE as SIMP_KELTNER_LOW
     , a.IND_VALUE2 as CALC_KELTNER_HIGH, b.IND_VALUE2 as SIMP_KELTNER_HIGH
     , a.AACTION as CALC_AACTION, b.AACTION as SIMP_AACTION
from table (HABR_MARKETINDEXES_KELTNER_F_CALC (5)) a
full outer join table (HABR_MARKETINDEXES_KELTNER_F_SIMP (5)) b on a.STOCK_NAME = b.STOCK_NAME and a.ADATE = b.ADATE
where sys_op_map_nonnull (a.ACLOSE) <> sys_op_map_nonnull (b.ACLOSE)
   or sys_op_map_nonnull (a.IND_VALUE) <> sys_op_map_nonnull (b.IND_VALUE)
   or sys_op_map_nonnull (a.IND_VALUE2) <> sys_op_map_nonnull (b.IND_VALUE2)
   or sys_op_map_nonnull (a.AACTION) <> sys_op_map_nonnull (b.AACTION)
order by 1, 2
;



--******************************************************************************
-- Indicator PVT, Price-Volume Trend
--******************************************************************************

select COLUMN_VALUE as ALG, dbms_sqlhash.gethash (COLUMN_VALUE, 2) as RECORDSET_HASH
from table (sys.odcivarchar2list ('select * from table (HABR_MARKETINDEXES_PVT_F_CALC (5)) order by 1, 2'
                                , 'select * from table (HABR_MARKETINDEXES_PVT_F_SIMP (5)) order by 1, 2'
                                , 'select * from table (HABR_MARKETINDEXES_PVT_F_MODE (5)) order by 1, 2'));

select coalesce (a.STOCK_NAME, b.STOCK_NAME) as STOCK_NAME, coalesce (a.ADATE, b.ADATE) as ADATE
     , a.ACLOSE as CALC_ACLOSE, b.ACLOSE as SIMP_CLOSE
     , a.IND_VALUE as CALC_OBV, b.IND_VALUE as SIMP_OBV
     , a.IND_VALUE2 as CALC_OBV_AVG, b.IND_VALUE2 as SIMP_OBV_AVG
     , a.AACTION as CALC_AACTION, b.AACTION as SIMP_AACTION
from            table (HABR_MARKETINDEXES_PVT_F_CALC (5)) a
full outer join table (HABR_MARKETINDEXES_PVT_F_SIMP (5)) b on a.STOCK_NAME = b.STOCK_NAME and a.ADATE = b.ADATE
where sys_op_map_nonnull (a.ACLOSE) <> sys_op_map_nonnull (b.ACLOSE)
   or sys_op_map_nonnull (a.IND_VALUE) <> sys_op_map_nonnull (b.IND_VALUE)
   or sys_op_map_nonnull (a.IND_VALUE2) <> sys_op_map_nonnull (b.IND_VALUE2)
   or sys_op_map_nonnull (a.AACTION) <> sys_op_map_nonnull (b.AACTION)
order by 1, 2
;



--******************************************************************************
-- Indicator EMV, Arms’ Ease of Movement Value
--******************************************************************************

select COLUMN_VALUE as ALG, dbms_sqlhash.gethash (COLUMN_VALUE, 2) as RECORDSET_HASH
from table (sys.odcivarchar2list ('select * from table (HABR_MARKETINDEXES_EMV_F_CALC (15)) order by 1, 2'
                                , 'select * from table (HABR_MARKETINDEXES_EMV_F_SIMP (15)) order by 1, 2'));


select coalesce (a.STOCK_NAME, b.STOCK_NAME) as STOCK_NAME, coalesce (a.ADATE, b.ADATE) as ADATE
     , a.ACLOSE as CALC_ACLOSE, b.ACLOSE as SIMP_CLOSE
     , a.IND_VALUE as CALC_EMV, b.IND_VALUE as SIMP_EMV
     , a.IND_VALUE2 as CALC_EMA, b.IND_VALUE2 as SIMP_EMA
     , a.AACTION as CALC_AACTION, b.AACTION as SIMP_AACTION
     , a.IND_VALUE2 - b.IND_VALUE2
from table (HABR_MARKETINDEXES_EMV_F_CALC (15)) a
full outer join table (HABR_MARKETINDEXES_EMV_F_SIMP (15)) b on a.STOCK_NAME = b.STOCK_NAME and a.ADATE = b.ADATE
where sys_op_map_nonnull (a.ACLOSE) <> sys_op_map_nonnull (b.ACLOSE)
   or sys_op_map_nonnull (a.IND_VALUE) <> sys_op_map_nonnull (b.IND_VALUE)
   or sys_op_map_nonnull (a.IND_VALUE2) <> sys_op_map_nonnull (b.IND_VALUE2)
   or sys_op_map_nonnull (a.AACTION) <> sys_op_map_nonnull (b.AACTION)
order by 1, 2;
;



--******************************************************************************
-- Indicator CCI, Commodity Channel Index
--******************************************************************************

select COLUMN_VALUE as ALG, dbms_sqlhash.gethash (COLUMN_VALUE, 2) as RECORDSET_HASH
from table (sys.odcivarchar2list ('select * from table (HABR_MARKETINDEXES_CCI_F_CALC (10)) order by 1, 2'
                                , 'select * from table (HABR_MARKETINDEXES_CCI_F_SIMP (10)) order by 1, 2'));

select coalesce (a.STOCK_NAME, b.STOCK_NAME) as STOCK_NAME, coalesce (a.ADATE, b.ADATE) as ADATE
     , a.ACLOSE as CALC_ACLOSE, b.ACLOSE as SIMP_CLOSE
     , a.IND_VALUE as CALC_CCI, b.IND_VALUE as SIMP_CCI
     , a.IND_VALUE2 as CALC_SMA, b.IND_VALUE2 as SIMP_SMA
     , a.IND_VALUE3 as CALC_MAD, b.IND_VALUE3 as SIMP_MAD
     , a.AACTION as CALC_AACTION, b.AACTION as SIMP_AACTION
from table (HABR_MARKETINDEXES_CCI_F_CALC (10)) a
full outer join table (HABR_MARKETINDEXES_CCI_F_SIMP (10)) b on a.STOCK_NAME = b.STOCK_NAME and a.ADATE = b.ADATE
where sys_op_map_nonnull (a.ACLOSE) <> sys_op_map_nonnull (b.ACLOSE)
   or sys_op_map_nonnull (a.IND_VALUE) <> sys_op_map_nonnull (b.IND_VALUE)
   or sys_op_map_nonnull (a.IND_VALUE2) <> sys_op_map_nonnull (b.IND_VALUE2)
   or sys_op_map_nonnull (a.IND_VALUE3) <> sys_op_map_nonnull (b.IND_VALUE3)
   or sys_op_map_nonnull (a.AACTION) <> sys_op_map_nonnull (b.AACTION)
order by 1, 2;




