--******************************************************************************
-- Indicator EMASIMPLE
--******************************************************************************

create or replace function HABR_MARKETINDEXES_EMASIMPLE_F_CALC (p_averaging_window_width integer)
return HABR_MARKETINDEXES_RESULT_LIST_T pipelined is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
    EMA number;
    prev_EMA number;
    prev_TYPICAL_PRICE number;
    retval HABR_MARKETINDEXES_RESULT_T := HABR_MARKETINDEXES_RESULT_T (null, null, null, null, null, null, null, null, null);
    prev_STOCK_NAME varchar2(256);
    l_alpha number;
begin

    l_alpha := 2 / (p_averaging_window_width + 1);

    for c1 in (select STOCK_NAME, ADATE, TYPICAL_PRICE, ACLOSE from LOAD_YAHOO_V order by 1, 2)
    loop
    
        retval.ADATE        := c1.ADATE;
        retval.ACLOSE       := c1.ACLOSE;

        if prev_STOCK_NAME is null or prev_STOCK_NAME <> c1.STOCK_NAME
        then
            retval.STOCK_NAME   := c1.STOCK_NAME;
            EMA                 := c1.TYPICAL_PRICE;
            prev_EMA            := null;
        else
            EMA := round (c1.TYPICAL_PRICE * l_alpha + EMA * (1 - l_alpha), 20);
        end if;
        
        if    prev_TYPICAL_PRICE < prev_EMA and c1.TYPICAL_PRICE > EMA then retval.AACTION := 'BUY';
        elsif prev_TYPICAL_PRICE > prev_EMA and c1.TYPICAL_PRICE < EMA then retval.AACTION := 'SELL';
        else  retval.AACTION := null;
        end if;

        retval.IND_VALUE  := EMA;
        
        pipe row (retval);
        
        prev_STOCK_NAME := c1.STOCK_NAME;
        prev_EMA := EMA;
        prev_TYPICAL_PRICE := c1.TYPICAL_PRICE;
        
    end loop;
end;



create or replace function HABR_MARKETINDEXES_EMASIMPLE_F_RECU (p_averaging_window_width integer)
return HABR_MARKETINDEXES_RESULT_LIST_T is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
begin

    with
      T1 (STOCK_NAME, ADATE, TYPICAL_PRICE, EMA, ACLOSE, RN) as 
           (select STOCK_NAME, ADATE, TYPICAL_PRICE, round (TYPICAL_PRICE, 20), ACLOSE, RN from LOAD_YAHOO_V where RN = 1
            union all
            select b.STOCK_NAME
                 , b.ADATE
                 , b.TYPICAL_PRICE
                 , round (b.TYPICAL_PRICE * 2 / (p_averaging_window_width + 1) + a.EMA * (1 - 2 / (p_averaging_window_width + 1)), 20)
                 , b.ACLOSE 
                 , b.RN
            from T1 a, LOAD_YAHOO_V b
            where b.RN = a.RN + 1 and b.STOCK_NAME = a.STOCK_NAME)
    select HABR_MARKETINDEXES_RESULT_T (STOCK_NAME, ADATE, ACLOSE, EMA, null, null, null, null, AACTION)
    bulk collect into l_result
    from T1 match_recognize (partition by STOCK_NAME order by ADATE
                             measures classifier() as AACTION
                             all rows per match with unmatched rows
                             pattern (BUY+ | SELL+)
                             define BUY  as (prev (TYPICAL_PRICE) < prev (EMA) and TYPICAL_PRICE > EMA)
                                  , SELL as (prev (TYPICAL_PRICE) > prev (EMA) and TYPICAL_PRICE < EMA)
                             ) MR;

    return l_result;
end;



create or replace function HABR_MARKETINDEXES_EMASIMPLE_F_MODE (p_averaging_window_width integer)
return HABR_MARKETINDEXES_RESULT_LIST_T is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
begin

    with
      T1 as (select * from LOAD_YAHOO_V
             model dimension by (STOCK_NAME, RN) measures (ADATE, TYPICAL_PRICE, ACLOSE, to_number(null) as EMA)
             rules (EMA[any, any] = round (TYPICAL_PRICE [cv(), cv()] * 2 / (p_averaging_window_width     + 1) + nvl(EMA [cv(), cv() - 1], TYPICAL_PRICE [cv(), cv()]) * (1 - 2 / (p_averaging_window_width     + 1)), 20)))
    , T2 as (select STOCK_NAME, ADATE, ACLOSE
                  , TYPICAL_PRICE, LAG (TYPICAL_PRICE) over (partition by STOCK_NAME order by ADATE) as PREV_TYPICAL_PRICE
                  , EMA, lag (EMA) over (partition by STOCK_NAME order by ADATE) as PREV_EMA
             from T1)
    select HABR_MARKETINDEXES_RESULT_T (STOCK_NAME, ADATE, ACLOSE, EMA, null, null, null, null
                                      , case when prev_TYPICAL_PRICE < prev_EMA and TYPICAL_PRICE > EMA then 'BUY'
                                             when prev_TYPICAL_PRICE > prev_EMA and TYPICAL_PRICE < EMA then 'SELL' end)
    bulk collect into l_result
    from T2 order by STOCK_NAME, ADATE;
    
    return l_result;

end;



create or replace type EMA_DATA_T as object (AVALUE number, AVERAGING_WINDOW integer);

create or replace type EMA_IMPL_T as object
(
    l_window_width integer,
    l_ema  number,
    static function ODCIAggregateInitialize   (sctx in out EMA_IMPL_T) return number,
    member function ODCIAggregateIterate      (self in out EMA_IMPL_T, value in EMA_DATA_T) return number,
    member function ODCIAggregateMerge        (self in out EMA_IMPL_T, ctx2 in EMA_IMPL_T) return number,
    member function ODCIAggregateTerminate    (self in EMA_IMPL_T, returnValue out number, flags in number) return number
);


create or replace type body EMA_IMPL_T is
 
static function ODCIAggregateInitialize (sctx in out EMA_IMPL_T) return number is
begin
    sctx := EMA_IMPL_T (null, null);
    return ODCIConst.Success;
end;

member function ODCIAggregateIterate (self in out EMA_IMPL_T, value in EMA_DATA_T) return number is
begin

    if value.AVALUE is not null
    then

        if l_window_width is null
        then
            l_window_width := value.AVERAGING_WINDOW;
            self.l_ema := value.AVALUE;
        else
            self.l_ema := round (value.AVALUE * 2 / (l_window_width  + 1) + self.l_ema * (1 - 2 / (l_window_width  + 1)), 20);
        end if;
    end if;
    
    return ODCIConst.Success;
end;

member function ODCIAggregateMerge(self in out EMA_IMPL_T, ctx2 in EMA_IMPL_T) return number is
begin
    return ODCIConst.Error;
end;

member function ODCIAggregateTerminate(self in EMA_IMPL_T, returnValue out number, flags in number) return number is
begin
    returnValue := self.l_ema;
    return ODCIConst.Success;
end;

end;

create or replace function EMA (input EMA_DATA_T) return number aggregate using EMA_IMPL_T;



create or replace function HABR_MARKETINDEXES_EMASIMPLE_F_AGRF (p_averaging_window_width integer)
return HABR_MARKETINDEXES_RESULT_LIST_T is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
begin

    with
      T1 as (select STOCK_NAME, ADATE, TYPICAL_PRICE, ACLOSE
                  , round (EMA (EMA_DATA_T (TYPICAL_PRICE, p_averaging_window_width)) over (partition by STOCK_NAME order by ADATE), 20) as EMA
             from LOAD_YAHOO_V)
    select HABR_MARKETINDEXES_RESULT_T (STOCK_NAME, ADATE, ACLOSE, EMA, null, null, null, null, AACTION)
    bulk collect into l_result
    from T1 match_recognize (partition by STOCK_NAME order by ADATE
                             measures classifier() as AACTION
                             all rows per match with unmatched rows
                             pattern (BUY+ | SELL+)
                             define BUY  as (prev (TYPICAL_PRICE) < prev (EMA) and TYPICAL_PRICE > EMA)
                                  , SELL as (prev (TYPICAL_PRICE) > prev (EMA) and TYPICAL_PRICE < EMA)
                             ) MR;

    return l_result;
end;



--******************************************************************************
-- Indicator CROSSES
--******************************************************************************

create or replace function HABR_MARKETINDEXES_CROSSES_F_CALC (p_averaging_window_width integer)
return HABR_MARKETINDEXES_RESULT_LIST_T pipelined is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
    EMAS number;
    prev_EMAS number;
    EMAL number;
    prev_EMAL number;
    retval HABR_MARKETINDEXES_RESULT_T := HABR_MARKETINDEXES_RESULT_T (null, null, null, null, null, null, null, null, null);
    prev_STOCK_NAME varchar2(256);
    l_alpha_short number;
    l_alpha_long number;
begin

    l_alpha_short := 2 / (p_averaging_window_width     + 1);
    l_alpha_long  := 2 / (p_averaging_window_width * 4 + 1);

    for c1 in (select STOCK_NAME, ADATE, TYPICAL_PRICE, ACLOSE from LOAD_YAHOO_V order by 1, 2)
    loop
    
        retval.ADATE        := c1.ADATE;
        retval.ACLOSE       := c1.ACLOSE;

        if prev_STOCK_NAME is null or prev_STOCK_NAME <> c1.STOCK_NAME
        then
            retval.STOCK_NAME   := c1.STOCK_NAME;
            EMAS      := c1.TYPICAL_PRICE;
            EMAL      := c1.TYPICAL_PRICE;
            prev_EMAS := null;
            prev_EMAL := null;
        else
            EMAS := round (c1.TYPICAL_PRICE * l_alpha_short + EMAS * (1 - l_alpha_short), 20);
            EMAL := round (c1.TYPICAL_PRICE * l_alpha_long  + EMAL * (1 - l_alpha_long), 20);

        end if;

        if    prev_EMAS < prev_EMAL and EMAS > EMAL then retval.AACTION := 'BUY';
        elsif prev_EMAS > prev_EMAL and EMAS < EMAL then retval.AACTION := 'SELL';
        else  retval.AACTION := null;
        end if;       

        retval.IND_VALUE  := EMAS;
        retval.IND_VALUE2 := EMAL;
        
        pipe row (retval);
        
        prev_STOCK_NAME := c1.STOCK_NAME;
        prev_EMAS := EMAS;
        prev_EMAL := EMAL;

    end loop;
end;



--******************************************************************************
-- Indicator OBV, On-Balance Volume
--******************************************************************************


create or replace function HABR_MARKETINDEXES_OBV_F_CALC (p_averaging_interval integer)
return HABR_MARKETINDEXES_RESULT_LIST_T pipelined is
    type arr_t is table of number index by pls_integer;
    l_arr_obv arr_t;
    l_prev_STOCK_NAME varchar2(256);
    l_prev_ACLOSE number;
    l_obv number;
    l_obv_avg_sum number;
    l_obv_avg number;
    l_prev_obv number;
    l_prev_obv_avg number;
begin

    for c1 in (select * from LOAD_YAHOO_V order by 1, 2)
    loop
    
        if l_prev_STOCK_NAME <> c1.STOCK_NAME or l_prev_STOCK_NAME is null
        then
            l_obv := 0;
            l_prev_ACLOSE := null;
            l_obv_avg_sum := 0;
            l_prev_obv_avg := null;
            l_arr_obv.delete;
        end if;
        
        if    c1.ACLOSE > l_prev_ACLOSE then l_obv := l_obv + c1.AVOLUME;
        elsif c1.ACLOSE < l_prev_ACLOSE then l_obv := l_obv - c1.AVOLUME;
        end if;

        l_arr_obv (nvl (l_arr_obv.last, 0) + 1) := l_obv;
        l_obv_avg_sum := l_obv_avg_sum + l_obv;
        
        if l_arr_obv.count > p_averaging_interval
        then
            l_obv_avg_sum := l_obv_avg_sum - l_arr_obv (l_arr_obv.first);
            l_arr_obv.delete (l_arr_obv.first);
        end if;

        l_obv_avg := round (l_obv_avg_sum / l_arr_obv.count, 20);

        pipe row (HABR_MARKETINDEXES_RESULT_T(c1.STOCK_NAME, c1.ADATE, c1.ACLOSE, l_obv, l_obv_avg, null, null, null
                , case when l_prev_obv < l_prev_obv_avg and l_obv > l_obv_avg then 'BUY'
                       when l_prev_obv > l_prev_obv_avg and l_obv < l_obv_avg then 'SELL' end));

        l_prev_STOCK_NAME := c1.STOCK_NAME;
        l_prev_ACLOSE := c1.ACLOSE;
        l_prev_obv := l_obv;
        l_prev_obv_avg := l_obv_avg;

    end loop;

end;



create or replace function HABR_MARKETINDEXES_OBV_F_SIMP (p_averaging_interval integer)
return HABR_MARKETINDEXES_RESULT_LIST_T is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
begin

    with
      T1 as (select STOCK_NAME, ADATE, ACLOSE
                  , lag (ACLOSE) over (partition by STOCK_NAME order by ADATE) as PREV_ACLOSE
                  , AVOLUME
             from LOAD_YAHOO)
    , T2 as (select STOCK_NAME, ADATE, ACLOSE
                  , nvl (sum (case when ACLOSE > PREV_ACLOSE then AVOLUME
                                   when ACLOSE < PREV_ACLOSE then - AVOLUME
                                   when ACLOSE = PREV_ACLOSE then 0 end) over (partition by STOCK_NAME order by ADATE), 0) as OBV
             from T1)
    , T3 as (select STOCK_NAME
                  , ADATE
                  , ACLOSE
                  , OBV
                  , round (avg (OBV) over (partition by STOCK_NAME order by ADATE rows between p_averaging_interval - 1 preceding and current row), 20) as OBV_AVG
             from T2)
    select HABR_MARKETINDEXES_RESULT_T (STOCK_NAME, ADATE, ACLOSE, OBV, OBV_AVG, null, null, null, AACTION)
    bulk collect into l_result
    from T3 match_recognize (partition by STOCK_NAME order by ADATE
                             measures classifier() as AACTION
                             all rows per match with unmatched rows
                             pattern (BUY+ | SELL+)
                             define BUY  as (prev (OBV) < prev (OBV_AVG) and OBV > OBV_AVG)
                                  , SELL as (prev (OBV) > prev (OBV_AVG) and OBV < OBV_AVG)
                             ) MR;

    return l_result;

end;



create or replace function HABR_MARKETINDEXES_OBV_F_MODE (p_averaging_interval integer)
return HABR_MARKETINDEXES_RESULT_LIST_T is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
begin

    with
      T1 as (select * from LOAD_YAHOO_V
             model partition by (STOCK_NAME) dimension by (RN) measures (ACLOSE, AVOLUME, to_number (null) as OBV, to_number (null) as OBV_AVG, cast (null as varchar2(10)) as AACTION)
             rules  (OBV [any] = nvl (OBV [cv() - 1], 0) + case when ACLOSE [cv()] > ACLOSE [cv() - 1] then   AVOLUME [cv()]
                                                                when ACLOSE [cv()] < ACLOSE [cv() - 1] then - AVOLUME [cv()]
                                                                when ACLOSE [cv()] = ACLOSE [cv() - 1] then 0
                                                                else 0 end
                   , OBV_AVG [any] = round (avg (OBV) [RN between cv() - p_averaging_interval + 1 and cv()], 20)
                   , AACTION [any] = case when OBV [cv() - 1] < OBV_AVG [cv () - 1] and OBV [cv ()] > OBV_AVG [cv ()] then 'BUY'
                                          when OBV [cv() - 1] > OBV_AVG [cv () - 1] and OBV [cv ()] < OBV_AVG [cv ()] then 'SELL' end
                    )
            )
    select HABR_MARKETINDEXES_RESULT_T (a.STOCK_NAME, b.ADATE, a.ACLOSE, a.OBV, a.OBV_AVG, null, null, null, a.AACTION)
    bulk collect into l_result
    from T1 a, LOAD_YAHOO_V b
    where a.STOCK_NAME = b.STOCK_NAME and a.RN = b.RN
    order by a.STOCK_NAME, b.ADATE;

    return l_result;

end;



--******************************************************************************
-- Indicator KELTNER, Keltner Channel
--******************************************************************************


create or replace function HABR_MARKETINDEXES_KELTNER_F_CALC (p_averaging_interval integer)
return HABR_MARKETINDEXES_RESULT_LIST_T pipelined is
    type arr_t is table of number index by pls_integer;
    l_arr_tp arr_t;
    l_arr_tr arr_t;
    l_tp_avg_sum number;
    l_tr_avg_sum number;
    l_tp_avg number;
    l_tr_avg number;
    l_prev_tp_avg number;
    l_prev_tr_avg number;
    l_keltner_low number;
    l_keltner_high number;
    l_prev_keltner_low number;
    l_prev_keltner_high number;
    l_prev_STOCK_NAME varchar2(256);
    l_prev_TYPICAL_PRICE number;
begin

    for c1 in (select * from LOAD_YAHOO_V order by 1, 2)
    loop
    
        if l_prev_STOCK_NAME <> c1.STOCK_NAME or l_prev_STOCK_NAME is null
        then
            l_prev_TYPICAL_PRICE := null;
            l_tp_avg_sum := 0;
            l_tr_avg_sum := 0;
            l_arr_tp.delete;
            l_arr_tr.delete;
            l_prev_keltner_low := null;
            l_prev_keltner_high := null;
        end if;
        
        l_arr_tp (nvl (l_arr_tp.last, 0) + 1) := c1.TYPICAL_PRICE;
        l_tp_avg_sum := l_tp_avg_sum + c1.TYPICAL_PRICE;
        
        if l_arr_tp.count > p_averaging_interval
        then
            l_tp_avg_sum := l_tp_avg_sum - l_arr_tp (l_arr_tp.first);
            l_arr_tp.delete (l_arr_tp.first);
        end if;

        l_tp_avg := round (l_tp_avg_sum / l_arr_tp.count, 20);

        l_arr_tr (nvl (l_arr_tr.last, 0) + 1) := c1.AHIGH - c1.ALOW;
        l_tr_avg_sum := l_tr_avg_sum + c1.AHIGH - c1.ALOW;
        
        if l_arr_tr.count > p_averaging_interval
        then
            l_tr_avg_sum := l_tr_avg_sum - l_arr_tr (l_arr_tr.first);
            l_arr_tr.delete (l_arr_tr.first);
        end if;

        l_tr_avg := round (l_tr_avg_sum / l_arr_tr.count, 20);
        
        l_keltner_low := l_tp_avg - l_tr_avg;
        l_keltner_high := l_tp_avg + l_tr_avg;

        pipe row (HABR_MARKETINDEXES_RESULT_T(c1.STOCK_NAME, c1.ADATE, c1.ACLOSE, l_keltner_low, l_keltner_high, null, null, null
                , case when c1.TYPICAL_PRICE > l_keltner_high and not l_prev_typical_price > l_prev_keltner_high then 'BUY'
                       when c1.TYPICAL_PRICE < l_keltner_low  and not l_prev_typical_price < l_prev_keltner_low  then 'SELL' end));
        
        l_prev_STOCK_NAME := c1.STOCK_NAME;
        l_prev_TYPICAL_PRICE := c1.TYPICAL_PRICE;
        l_prev_keltner_low := l_keltner_low;
        l_prev_keltner_high := l_keltner_high;
    
    end loop;

end;



create or replace function HABR_MARKETINDEXES_KELTNER_F_SIMP (p_averaging_interval integer)
return HABR_MARKETINDEXES_RESULT_LIST_T is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
begin

    with
      T1 as (select STOCK_NAME
                  , ADATE
                  , ACLOSE
                  , TYPICAL_PRICE
                  , round (avg (TYPICAL_PRICE) over (partition by STOCK_NAME order by ADATE rows between p_averaging_interval - 1 preceding and current row), 20) as TYPICAL_PRICE_SMA
                  , round (avg (AHIGH - ALOW)  over (partition by STOCK_NAME order by ADATE rows between p_averaging_interval - 1 preceding and current row), 20) as TRADING_RANGE_SMA
             from LOAD_YAHOO_V)
    , T2 as (select STOCK_NAME
                  , ADATE
                  , ACLOSE
                  , TYPICAL_PRICE
                  , TYPICAL_PRICE_SMA - TRADING_RANGE_SMA as KELTNER_LOW
                  , TYPICAL_PRICE_SMA + TRADING_RANGE_SMA as KELTNER_HIGH
             from T1)
    select HABR_MARKETINDEXES_RESULT_T (STOCK_NAME, ADATE, ACLOSE, KELTNER_LOW, KELTNER_HIGH, null, null, null, AACTION)
    bulk collect into l_result
    from T2 match_recognize (partition by STOCK_NAME order by ADATE
                             measures classifier() as AACTION
                             all rows per match with unmatched rows
                             pattern (BUY+ | SELL+)
                             define BUY  as (TYPICAL_PRICE > KELTNER_HIGH and not prev (TYPICAL_PRICE) > prev (KELTNER_HIGH))
                                  , SELL as (TYPICAL_PRICE < KELTNER_LOW  and not prev (TYPICAL_PRICE) < prev (KELTNER_LOW))
                             ) MR;

    return l_result;
end;



create or replace function HABR_MARKETINDEXES_KELTNER_F_MODE (p_averaging_interval integer)
return HABR_MARKETINDEXES_RESULT_LIST_T is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
begin

    with
      T1 as (select *
             from LOAD_YAHOO_V
             model partition by (STOCK_NAME) dimension by (RN) measures (ACLOSE, TYPICAL_PRICE, AHIGH - ALOW as TRADING_RANGE
                           , round (avg (TYPICAL_PRICE) over (partition by STOCK_NAME order by ADATE rows between p_averaging_interval - 1 preceding and current row), 20) as TYPICAL_PRICE_SMA
                           , round (avg (AHIGH - ALOW)  over (partition by STOCK_NAME order by ADATE rows between p_averaging_interval - 1 preceding and current row), 20) as TRADING_RANGE_SMA
                           , to_number (null) as KELTNER_LOW
                           , to_number (null) as KELTNER_HIGH
                           , cast (null as varchar2(4)) as AACTION
             )
             rules (
                   KELTNER_LOW  [any] = TYPICAL_PRICE_SMA [cv ()] - TRADING_RANGE_SMA [cv ()]
                 , KELTNER_HIGH [any] = TYPICAL_PRICE_SMA [cv ()] + TRADING_RANGE_SMA [cv ()]
                 , AACTION [any] = case when TYPICAL_PRICE [cv ()] > KELTNER_HIGH [cv ()] and not TYPICAL_PRICE [cv () - 1] > KELTNER_HIGH [cv () - 1] then 'BUY'
                                        when TYPICAL_PRICE [cv ()] < KELTNER_LOW  [cv ()] and not TYPICAL_PRICE [cv () - 1] < KELTNER_LOW  [cv () - 1] then 'SELL' end
             )
            )
    select HABR_MARKETINDEXES_RESULT_T (a.STOCK_NAME, b.ADATE, a.ACLOSE, a.KELTNER_LOW, a.KELTNER_HIGH, null, null, null, a.AACTION)
    bulk collect into l_result
    from T1 a join LOAD_YAHOO_V b on a.STOCK_NAME = b.STOCK_NAME and a.RN = b.RN 
    order by a.STOCK_NAME, b.ADATE;

    return l_result;

end;



--******************************************************************************
-- Indicator PVT, Price-Volume Trend
--******************************************************************************


create or replace function HABR_MARKETINDEXES_PVT_F_CALC (p_averaging_interval integer)
return HABR_MARKETINDEXES_RESULT_LIST_T pipelined is
    type arr_t is table of number index by pls_integer;
    l_arr_pvt arr_t;
    l_prev_STOCK_NAME varchar2(256);
    l_prev_ACLOSE number;
    l_pvt number;
    l_pvt_avg_sum number;
    l_pvt_avg number;
    l_prev_pvt number;
    l_prev_pvt_avg number;
begin

    for c1 in (select * from LOAD_YAHOO_V order by 1, 2)
    loop
    
        if l_prev_STOCK_NAME <> c1.STOCK_NAME or l_prev_STOCK_NAME is null
        then
            l_arr_pvt.delete;
            l_prev_ACLOSE := null;
            l_pvt := 0;
            l_pvt_avg_sum := 0; 
            l_prev_pvt := null;
            l_prev_pvt_avg := null;
        end if;

        if l_prev_ACLOSE is not null
        then
            l_pvt := l_pvt + round (c1.AVOLUME * (c1.ACLOSE - l_prev_ACLOSE) / l_prev_ACLOSE, 20);
        end if; 

        l_arr_pvt (nvl (l_arr_pvt.last, 0) + 1) := l_pvt;
        l_pvt_avg_sum := l_pvt_avg_sum + l_pvt;
        
        if l_arr_pvt.count > p_averaging_interval
        then
            l_pvt_avg_sum := l_pvt_avg_sum - l_arr_pvt (l_arr_pvt.first);
            l_arr_pvt.delete (l_arr_pvt.first);
        end if;

        l_pvt_avg := round (l_pvt_avg_sum / l_arr_pvt.count, 20);

        pipe row (HABR_MARKETINDEXES_RESULT_T(c1.STOCK_NAME, c1.ADATE, c1.ACLOSE, l_pvt, l_pvt_avg, null, null, null
                , case when l_pvt > l_pvt_avg and l_prev_pvt < l_prev_pvt_avg then 'BUY'
                       when l_pvt < l_pvt_avg and l_prev_pvt > l_prev_pvt_avg then 'SELL' end));
        
        l_prev_STOCK_NAME := c1.STOCK_NAME;
        l_prev_ACLOSE := c1.ACLOSE;
        l_prev_pvt := l_pvt;
        l_prev_pvt_avg := l_pvt_avg;
    
    end loop;

end;



create or replace function HABR_MARKETINDEXES_PVT_F_SIMP (p_averaging_interval integer)
return HABR_MARKETINDEXES_RESULT_LIST_T is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
begin

    with
      T1 as (select STOCK_NAME, ADATE
                  , nvl (round (AVOLUME * (ACLOSE - lag (ACLOSE) over (partition by STOCK_NAME order by ADATE)) / lag (ACLOSE) over (partition by STOCK_NAME order by ADATE), 20), 0) as PVT_CUR
                  , ACLOSE
             from LOAD_YAHOO_V)
    , T2 as (select STOCK_NAME, ADATE
                  , sum (PVT_CUR) over (partition by STOCK_NAME order by ADATE) as PVT
                  , ACLOSE
             from T1)
    , T3 as (select STOCK_NAME, ADATE, ACLOSE, PVT
                  , round (avg (PVT) over (partition by STOCK_NAME order by ADATE rows between p_averaging_interval - 1 preceding and current row), 20) as PVT_AVG
             from T2)
    , T4 as (select STOCK_NAME, ADATE, ACLOSE
                  , PVT,     lag (PVT)     over (partition by STOCK_NAME order by ADATE) as PREV_PVT 
                  , PVT_AVG, lag (PVT_AVG) over (partition by STOCK_NAME order by ADATE) as PREV_PVT_AVG
             from T3) 
    select HABR_MARKETINDEXES_RESULT_T (STOCK_NAME, ADATE, ACLOSE, nvl(PVT, 0), PVT_AVG, null, null, null
         , case when PVT > PVT_AVG and PREV_PVT < PREV_PVT_AVG then 'BUY'
                when PVT < PVT_AVG and PREV_PVT > PREV_PVT_AVG then 'SELL'
           end
)
    bulk collect into l_result
    from T4
    order by STOCK_NAME, ADATE;

    return l_result;

end;



create or replace function HABR_MARKETINDEXES_PVT_F_MODE (p_averaging_interval integer)
return HABR_MARKETINDEXES_RESULT_LIST_T is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
begin

    with
      T1 as (select *
             from LOAD_YAHOO_V
             model partition by (STOCK_NAME) dimension by (RN) measures (ACLOSE, AVOLUME, to_number (null) as PVT, to_number (null) as PVT_AVG, cast (null as varchar2(4)) as AACTION)
             rules (
                   PVT [1] = 0
                 , PVT [RN > 1] = nvl (PVT [cv () - 1], 0) + nvl (round (AVOLUME [cv ()] * (ACLOSE [cv ()] - ACLOSE [cv () - 1]) / ACLOSE [cv () - 1], 20), 0)
                 , PVT_AVG [any] = round (avg (PVT) [RN between cv () - p_averaging_interval + 1 and cv()], 20)
                 , AACTION [any] = case when PVT [cv ()] > PVT_AVG [cv ()] and PVT [cv () - 1] < PVT_AVG [cv () - 1] then 'BUY'
                                        when PVT [cv ()] < PVT_AVG [cv ()] and PVT [cv () - 1] > PVT_AVG [cv () - 1] then 'SELL' end
             )
            )
    select HABR_MARKETINDEXES_RESULT_T (a.STOCK_NAME, b.ADATE, a.ACLOSE, a.PVT, a.PVT_AVG, null, null, null, a.AACTION)
    bulk collect into l_result
    from T1 a, LOAD_YAHOO_V b
    where a.STOCK_NAME = b.STOCK_NAME and a.RN = b.RN
    order by a.STOCK_NAME, b.ADATE;

    return l_result;

end;



--******************************************************************************
-- Indicator EMV, Arms’ Ease of Movement Value
--******************************************************************************


create or replace function HABR_MARKETINDEXES_EMV_F_CALC (p_averaging_window_width integer)
return HABR_MARKETINDEXES_RESULT_LIST_T pipelined is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
    l_EMA number;
    l_EMV number;
    l_prev_EMA number;
    l_prev_ALOW number;
    l_prev_AHIGH number;
    retval HABR_MARKETINDEXES_RESULT_T := HABR_MARKETINDEXES_RESULT_T (null, null, null, null, null, null, null, null, null);
    prev_STOCK_NAME varchar2(256);
    l_alpha number;
begin

    l_alpha := 2 / (p_averaging_window_width     + 1);

    for c1 in (select * from LOAD_YAHOO order by 1, 2)
    loop
    
        retval.ADATE        := c1.ADATE;
        retval.ACLOSE       := c1.ACLOSE;

        if prev_STOCK_NAME is null or prev_STOCK_NAME <> c1.STOCK_NAME
        then
            retval.STOCK_NAME   := c1.STOCK_NAME;
            l_prev_EMA          := null;
            l_prev_ALOW         := null;
            l_prev_AHIGH        := null;
            l_EMV := null;
            l_EMA := null;
        end if;
        
        if prev_STOCK_NAME is null or prev_STOCK_NAME <> c1.STOCK_NAME
        then
            null;
        else

            if c1.AVOLUME > 0 and c1.AHIGH - c1.ALOW > 0
            then
                l_EMV := round (((c1.AHIGH + c1.ALOW) / 2 - (l_prev_AHIGH + l_prev_ALOW) / 2) / (c1.AVOLUME / (c1.AHIGH - c1.ALOW)), 20);

                if l_EMA is null
                then
                    l_EMA := l_EMV;
                else
                    l_EMA := round (l_EMV * l_alpha + l_EMA * (1 - l_alpha), 20);
                end if;

            else
                l_EMV := null;
            end if;

        end if;

        if    l_EMA > 0 and l_prev_EMA <= 0 then retval.AACTION := 'BUY';
        elsif l_EMA < 0 and l_prev_EMA >= 0 then retval.AACTION := 'SELL';
        else retval.AACTION := null;
        end if;

        retval.IND_VALUE  := l_EMV;
        retval.IND_VALUE2 := l_EMA;
        
        pipe row (retval);
        
        prev_STOCK_NAME := c1.STOCK_NAME;
        l_prev_EMA := l_EMA;
        l_prev_ALOW := c1.ALOW;
        l_prev_AHIGH := c1.AHIGH;
        
    end loop;
end;



create or replace function HABR_MARKETINDEXES_EMV_F_SIMP (p_averaging_interval integer)
return HABR_MARKETINDEXES_RESULT_LIST_T is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
begin

    with
      T1 as (select a.*
                  , lag (AHIGH) over (partition by STOCK_NAME order by ADATE) as PREV_AHIGH 
                  , lag (ALOW)  over (partition by STOCK_NAME order by ADATE) as PREV_ALOW 
             from LOAD_YAHOO a)
    , T2 as (select STOCK_NAME, ADATE, ACLOSE, round (((AHIGH + ALOW) / 2 - (PREV_AHIGH + PREV_ALOW) / 2) / case when AVOLUME > 0 then (AVOLUME / (case when AHIGH - ALOW > 0 then AHIGH - ALOW end)) end, 20) as EMV from T1)
    , T3 as (select STOCK_NAME, ADATE, ACLOSE, EMV
                  , EMA (EMA_DATA_T (EMV, p_averaging_interval)) over (partition by STOCK_NAME order by ADATE) as EMV_EMA
             from T2)
    select HABR_MARKETINDEXES_RESULT_T (STOCK_NAME, ADATE, ACLOSE, EMV, EMV_EMA, null, null, null, AACTION)
    bulk collect into l_result
    from T3 match_recognize (partition by STOCK_NAME order by ADATE
                             measures classifier() AS AACTION
                             all rows per match with unmatched rows
                             pattern (BUY+|SELL+)
                             define BUY  AS (EMV_EMA > 0 and prev(EMV_EMA) <= 0)
                                  , SELL AS (EMV_EMA < 0 and prev(EMV_EMA) >= 0)
    );

    return l_result;

end;


    
--******************************************************************************
-- Indicator CCI, Commodity Channel Index
--******************************************************************************


create or replace function HABR_MARKETINDEXES_CCI_F_CALC (p_averaging_window integer)
return HABR_MARKETINDEXES_RESULT_LIST_T pipelined is
    type arr_t is table of number index by pls_integer;
    l_arr_tp arr_t;
    l_arr_sma arr_t;
    l_sma_sum number;
    l_sma number;
    l_prev_STOCK_NAME varchar2(256);
    l_mad_sum number;
    l_mad number;
    l_cci number;
    l_prev_cci number;
begin

    for c1 in (select * from LOAD_YAHOO_V order by 1, 2)
    loop
    
        if c1.STOCK_NAME <> l_prev_STOCK_NAME or l_prev_STOCK_NAME is null
        then
            l_sma_sum := 0;
            l_arr_tp.delete;
            l_arr_sma.delete;
        end if;
    
        l_arr_tp (nvl (l_arr_tp.last, 0) + 1) := c1.TYPICAL_PRICE;
        l_sma_sum := l_sma_sum + c1.TYPICAL_PRICE;
        
        if l_arr_tp.count > p_averaging_window
        then
            l_sma_sum := l_sma_sum - l_arr_tp (l_arr_tp.first);
            l_arr_tp.delete (l_arr_tp.first);
        end if;

        l_sma := round (l_sma_sum / l_arr_tp.count, 20);
        
        l_arr_sma (nvl (l_arr_sma.last, 0) + 1) := l_sma;

        if l_arr_sma.count > p_averaging_window
        then
            l_arr_sma.delete (l_arr_sma.first);
        end if;

        l_mad_sum := 0;
        
        for i in l_arr_tp.first..l_arr_tp.last
        loop
        
            l_mad_sum := l_mad_sum + abs (l_arr_tp(i) - l_arr_sma (i));
        
        end loop;

        l_mad := round (l_mad_sum / l_arr_tp.count, 20);

        if l_mad <> 0 then l_cci := round ((1 / 0.015) * (c1.TYPICAL_PRICE - l_sma) / l_mad, 20); else l_cci := null; end if;

        pipe row (HABR_MARKETINDEXES_RESULT_T (c1.STOCK_NAME, c1.ADATE, c1.ACLOSE, l_cci, l_sma, l_mad, null, null
                                             , case when l_prev_cci <= 100 and l_cci > 100 then 'BUY'
                                                    when l_prev_cci >= 100 and l_cci < 100 then 'SELL' end));
        
        l_prev_STOCK_NAME := c1.STOCK_NAME;
        l_prev_cci := l_cci;
    
    end loop;

end;


create or replace function HABR_MARKETINDEXES_CCI_F_SIMP (p_averaging_interval integer)
return HABR_MARKETINDEXES_RESULT_LIST_T is
    l_result HABR_MARKETINDEXES_RESULT_LIST_T;
begin

    with
      T1 as (select STOCK_NAME
                  , ADATE 
                  , TYPICAL_PRICE
                  , round (avg (TYPICAL_PRICE) over (partition by STOCK_NAME order by ADATE rows between p_averaging_interval - 1 preceding and current row), 20) as SMA
                  , ACLOSE
                  from LOAD_YAHOO_V)
    , T2 as (select STOCK_NAME
                  , ADATE 
                  , TYPICAL_PRICE
                  , SMA
                  , ACLOSE
                  , round (avg (abs (TYPICAL_PRICE - SMA)) over (partition by STOCK_NAME order by ADATE rows between p_averaging_interval - 1 preceding and current row), 20) as MAD
                  from T1)
    , T3 as (select STOCK_NAME, ADATE, ACLOSE, SMA, MAD
                  , round ((1 / 0.015) * (TYPICAL_PRICE - SMA) / case when MAD <> 0 then MAD end, 20) as CCI
             from T2)
    select HABR_MARKETINDEXES_RESULT_T (STOCK_NAME, ADATE, ACLOSE, CCI, SMA, MAD, null, null, AACTION)
    bulk collect into l_result
    from T3 match_recognize (partition by STOCK_NAME order by ADATE
                             measures classifier() AS AACTION
                             all rows per match with unmatched rows
                             pattern (BUY+|SELL+)
                             define BUY  AS (prev(CCI) <= 100 and CCI > 100)
                                  , SELL AS (prev(CCI) >= 100 and CCI < 100)
    );

    return l_result;
    
end;




--******************************************************************************
-- WMA function without indicator, for future use
--******************************************************************************


create or replace type WMA_DATA_T as object (AVALUE number, AVERAGING_INTERVAL integer);
create or replace type WMA_ARRAY_T is table of number;

create or replace type WMA_IMPL_T as object
(
    l_window_width integer,
    l_wma  number,
    l_array WMA_ARRAY_T,
    static function ODCIAggregateInitialize   (sctx in out WMA_IMPL_T) return number,
    member function ODCIAggregateIterate      (self in out WMA_IMPL_T, value in WMA_DATA_T) return number,
    member function ODCIAggregateMerge        (self in out WMA_IMPL_T, ctx2 in WMA_IMPL_T) return number,
    member function ODCIAggregateTerminate    (self in WMA_IMPL_T, returnValue out number, flags in number) return number
);

create or replace type body WMA_IMPL_T is

static function ODCIAggregateInitialize (sctx in out WMA_IMPL_T) return number is
begin
    sctx := WMA_IMPL_T (null, null, WMA_ARRAY_T());
    return ODCIConst.Success;
end;

member function ODCIAggregateIterate (self in out WMA_IMPL_T, value in WMA_DATA_T) return number is
begin
    if l_window_width is null
    then
        l_window_width := value.AVERAGING_INTERVAL;
    end if;

    if (value.AVALUE is not null)
    then

        self.l_array.extend;
        self.l_array(self.l_array.last) := value.AVALUE;
    
        if self.l_array.count > l_window_width
        then
            self.l_array.delete (self.l_array.first);
        end if;
    end if;

    return ODCIConst.Success;
end;

member function ODCIAggregateMerge(self in out WMA_IMPL_T, ctx2 in WMA_IMPL_T) return number is
begin
    return ODCIConst.Error;
end;

member function ODCIAggregateTerminate(self in WMA_IMPL_T, returnValue out number, flags in number) return number is
    j number;
begin

    j := self.l_array.count;

    if self.l_array.count > 0
    then 

        for i in reverse self.l_array.first()..self.l_array.last()
        loop
            returnValue := nvl (returnValue, 0) + self.l_array(i) * j;
            j := j - 1;
        end loop;

        returnValue := returnValue / (self.l_array.count * (self.l_array.count + 1) / 2);
    else
        returnValue := null;
    end if;

    return ODCIConst.Success;
end;

end;

create or replace function WMA (input WMA_DATA_T) return number aggregate using WMA_IMPL_T;



