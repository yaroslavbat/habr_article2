create table LOAD_YAHOO (
      STOCK_NAME    varchar2(128)
    , ADATE         date
    , AOPEN         number
    , AHIGH         number
    , ALOW          number
    , ACLOSE        number check (ACLOSE > 0)
    , AVOLUME       number
    , constraint LOAD_YAHOO_PKIOT primary key (STOCK_NAME, ADATE)
) organization index compress 1;


create or replace view LOAD_YAHOO_V as
select STOCK_NAME, ADATE
     , row_number () over (partition by STOCK_NAME order by ADATE) as RN
     , AOPEN, AHIGH, ALOW, ACLOSE, AVOLUME, round ((AHIGH + ALOW + ACLOSE) / 3, 20) as TYPICAL_PRICE
from LOAD_YAHOO a;


create type HABR_MARKETINDEXES_RESULT_T
as object (STOCK_NAME varchar2(128), ADATE date, ACLOSE number
         , IND_VALUE number, IND_VALUE2 number, IND_VALUE3 number, IND_VALUE4 number, IND_VALUE5 number
         , AACTION varchar(4));
create type HABR_MARKETINDEXES_RESULT_LIST_T as table of HABR_MARKETINDEXES_RESULT_T;


create table HABR_MARKETINDEXES_PARMSEL_RESULTS (
      INDICATOR_NAME        varchar2(256)
    , PARM1                 number
    , PARM2                 number
    , STOCK_NAME            varchar2(128)
    , ADATE_MIN             date
    , ADATE_MAX             date
    , DEALS_COUNT           number
    , BALANCE_RESULT        number
    , DEALS_PROFIT_AMOUNT   number
    , DEALS_LOSS_AMOUNT     number
    , DEALS_PROFIT_COUNT    number
    , DEALS_LOSS_COUNT      number
    , IN_STOCK              varchar2(16)
    , constraint HABR_MARKETINDEXES_PARMSEL_RESULTS_PKIOT primary key (INDICATOR_NAME, PARM1, PARM2, STOCK_NAME)
) organization index compress 3;



--****************************** PACKAGE 

create or replace package HABR_TRADEMODELLING_P as

type ACTION_T is record (STOCK_NAME varchar2(128), ADATE date, APRICE number, AACTION varchar2(32));
type CURSOR_ACTIONS_T is ref cursor return ACTION_T;

-- Trade Log

type TRADE_LOG_T is record (STOCK_NAME varchar2(128), ADATE_LONG_OPEN date, ADATE_LONG_CLOSE date, DAYS_HELD integer, STOCK_COUNT number
                             , APRICE_BUY number, APRICE_SELL number, AAMOUNT_BUY number, AAMOUNT_SELL number, DEAL_PROFIT_AMOUNT number, DEAL_LOSS_AMOUNT number, IN_STOCK varchar2(32), BALANCE_RESULT number);
type TRADE_LOG_LIST_T is table of TRADE_LOG_T;

function TRADE_LOG (p_cursor CURSOR_ACTIONS_T, p_lag integer, p_initial_balance number default 1000) return TRADE_LOG_LIST_T
pipelined order p_cursor by (STOCK_NAME, ADATE) parallel_enable (partition p_cursor by hash (STOCK_NAME));


-- Calc Actions

type CALC_ACTIONS_T is record (STOCK_NAME varchar2(128), ADATE date, APRICE number, AACTION varchar2(32), AACTION_LAG varchar2(32), BALANCE_CURRENCY number, BALANCE_STOCK number);
type CALC_ACTIONS_LIST_T is table of CALC_ACTIONS_T;

function CALC_ACTIONS (p_cursor CURSOR_ACTIONS_T, p_lag integer, p_initial_balance number default 1000) return CALC_ACTIONS_LIST_T
pipelined order p_cursor by (STOCK_NAME, ADATE) parallel_enable (partition p_cursor by hash (STOCK_NAME));


-- Calc Actions Result

type CALC_ACTIONS_TOTALS_T
is record (STOCK_NAME varchar2(128), ADATE_MIN date, ADATE_MAX date, DEALS_COUNT integer, BALANCE_RESULT number
         , DEALS_PROFIT_AMOUNT number, DEALS_LOSS_AMOUNT number, DEALS_PROFIT_COUNT integer, DEALS_LOSS_COUNT integer
         , IN_STOCK varchar2(16));
type CALC_ACTIONS_TOTALS_LIST_T is table of CALC_ACTIONS_TOTALS_T;

function CALC_ACTIONS_TOTALS (p_cursor CURSOR_ACTIONS_T, p_lag integer, p_initial_balance number default 1000) return CALC_ACTIONS_TOTALS_LIST_T
pipelined order p_cursor by (STOCK_NAME, ADATE) parallel_enable (partition p_cursor by hash (STOCK_NAME));

end;
/

create or replace package body HABR_TRADEMODELLING_P AS

function TRADE_LOG (p_cursor CURSOR_ACTIONS_T, p_lag integer, p_initial_balance number default 1000) return TRADE_LOG_LIST_T
pipelined order p_cursor by (STOCK_NAME, ADATE) parallel_enable (partition p_cursor by hash (STOCK_NAME)) is
    l_aaction_lag               varchar2(10);
    c1                          ACTION_T;
    prev_c1                     ACTION_T;
    retval                      TRADE_LOG_T;
    type ACTION_HISTORY_T is table of varchar2(16);
    l_aaction_history ACTION_HISTORY_T := ACTION_HISTORY_T();
    procedure f_sale (p_sell_price number, p_sell_date date) is
    begin
        retval.APRICE_SELL        := p_sell_price;
        retval.ADATE_LONG_CLOSE   := p_sell_date;
        
        retval.AAMOUNT_SELL       := round (retval.APRICE_SELL * retval.STOCK_COUNT, 2);
        
        if - retval.AAMOUNT_BUY + retval.AAMOUNT_SELL > 0
        then retval.DEAL_PROFIT_AMOUNT := - retval.AAMOUNT_BUY + retval.AAMOUNT_SELL;
             retval.DEAL_LOSS_AMOUNT   := null;
        elsif - retval.AAMOUNT_BUY + retval.AAMOUNT_SELL < 0
        then retval.DEAL_PROFIT_AMOUNT := null;
             retval.DEAL_LOSS_AMOUNT   := retval.AAMOUNT_BUY - retval.AAMOUNT_SELL;
        else retval.DEAL_PROFIT_AMOUNT := null;
             retval.DEAL_LOSS_AMOUNT   := null;
        end if;
        retval.BALANCE_RESULT     := retval.BALANCE_RESULT - retval.AAMOUNT_BUY + retval.AAMOUNT_SELL;
    end;
begin

    loop

        fetch p_cursor into c1;
        exit when p_cursor%notfound;
        
        if c1.STOCK_NAME is null or c1.ADATE is null or c1.APRICE is null
        then
            raise_application_error (-20001, 'Fields STOCK_NAME, ADATE, APRICE must be not null.');
        end if;
         
        if prev_c1.STOCK_NAME is not null
          and (   (c1.STOCK_NAME < prev_c1.STOCK_NAME)
               or (c1.STOCK_NAME = prev_c1.STOCK_NAME and c1.ADATE < prev_c1.ADATE))
        then
            raise_application_error (-20001, 'Rowset must be ordered by STOCK_NAME, ADATE.');
        end if;
        
        if c1.STOCK_NAME <> prev_c1.STOCK_NAME or prev_c1.STOCK_NAME is null
        then

            if (retval.ADATE_LONG_OPEN is not null and retval.ADATE_LONG_CLOSE is null)
            then
                f_sale (prev_c1.APRICE, prev_c1.ADATE);
                retval.IN_STOCK       := 'In Stock';
                pipe row (retval);
            end if;

            retval.ADATE_LONG_OPEN    := null;
            retval.ADATE_LONG_CLOSE   := null;
            retval.BALANCE_RESULT   := p_initial_balance;
            retval.STOCK_NAME       := c1.STOCK_NAME;
            retval.IN_STOCK       := null;
            l_aaction_history.delete;            
            

        end if;
    
        l_aaction_history.extend(1);
        l_aaction_history(l_aaction_history.last) := c1.AACTION;
  
        if l_aaction_history.last > p_lag
        then
            l_aaction_lag := l_aaction_history (l_aaction_history.last - p_lag);
            if     l_aaction_lag = 'BUY'
            then
                retval.ADATE_LONG_OPEN    := c1.ADATE;
                retval.ADATE_LONG_CLOSE   := null;
                retval.STOCK_COUNT        := floor (1000 * retval.BALANCE_RESULT / c1.APRICE) / 1000;
                retval.APRICE_BUY         := c1.APRICE;
                retval.AAMOUNT_BUY        := round (c1.APRICE * retval.STOCK_COUNT, 2);
                retval.APRICE_SELL        := null;
                retval.AAMOUNT_SELL       := null;
            elsif l_aaction_lag = 'SELL' and retval.ADATE_LONG_OPEN is not null
            then
                f_sale (c1.APRICE, c1.ADATE);
                retval.DAYS_HELD := retval.ADATE_LONG_CLOSE - retval.ADATE_LONG_OPEN;
                pipe row (retval);
                retval.ADATE_LONG_OPEN    := null;
                retval.ADATE_LONG_CLOSE   := null;
            end if;
        end if;

        prev_c1 := c1;
   
    end loop;
    
    if (retval.ADATE_LONG_OPEN is not null and retval.ADATE_LONG_CLOSE is null)
    then
        f_sale (prev_c1.APRICE, prev_c1.ADATE);
        retval.IN_STOCK       := 'In Stock';
        pipe row (retval);
    end if;
    
    return;

end;


function CALC_ACTIONS (p_cursor CURSOR_ACTIONS_T, p_lag integer, p_initial_balance number default 1000) return CALC_ACTIONS_LIST_T
pipelined order p_cursor by (STOCK_NAME, ADATE) parallel_enable (partition p_cursor by hash (STOCK_NAME)) is
    l_deal_currency             number;
    l_deal_stock                number;
    c1                          ACTION_T;
    prev_c1                     ACTION_T;
    retval                      CALC_ACTIONS_T;
    type ACTION_HISTORY_T is table of varchar2(16);
    l_aaction_history ACTION_HISTORY_T := ACTION_HISTORY_T();
begin


    loop

        fetch p_cursor into c1;
        exit when p_cursor%notfound;
        
        if c1.STOCK_NAME is null or c1.ADATE is null or c1.APRICE is null
        then
            raise_application_error (-20001, 'Fields STOCK_NAME, ADATE, APRICE must be not null.');
        end if;

        if prev_c1.STOCK_NAME is not null
          and (   (c1.STOCK_NAME < prev_c1.STOCK_NAME)
               or (c1.STOCK_NAME = prev_c1.STOCK_NAME and c1.ADATE < prev_c1.ADATE))
        then
            raise_application_error (-20001, 'Rowset must be ordered by STOCK_NAME, ADATE.');
        end if;

        if c1.STOCK_NAME <> prev_c1.STOCK_NAME or prev_c1.STOCK_NAME is null
        then
            
            retval.BALANCE_CURRENCY := p_initial_balance;
            retval.BALANCE_STOCK := 0;
            retval.STOCK_NAME := c1.STOCK_NAME;
            l_aaction_history.delete;            

        end if;
    
        l_aaction_history.extend(1);
        l_aaction_history(l_aaction_history.last) := c1.AACTION;
  
        if l_aaction_history.last > p_lag
        then
            retval.AACTION_LAG := l_aaction_history(l_aaction_history.last - p_lag);
            
            if     retval.AACTION_LAG = 'BUY'
            then
                l_deal_stock := floor (1000 * retval.BALANCE_CURRENCY / c1.APRICE) / 1000;
                l_deal_currency := round (l_deal_stock * c1.APRICE, 2);
            
                retval.BALANCE_CURRENCY := retval.BALANCE_CURRENCY - l_deal_currency; 
                retval.BALANCE_STOCK := retval.BALANCE_STOCK + l_deal_stock;
             elsif retval.AACTION_LAG = 'SELL'
             then
                l_deal_currency := round (retval.BALANCE_STOCK * c1.APRICE, 2);
                l_deal_stock := retval.BALANCE_STOCK;
            
                retval.BALANCE_CURRENCY := retval.BALANCE_CURRENCY + l_deal_currency; 
                retval.BALANCE_STOCK := retval.BALANCE_STOCK - l_deal_stock;
            end if;
        else
            retval.AACTION_LAG := null;
        end if;

        retval.ADATE := c1.ADATE;
        retval.APRICE := c1.APRICE;
        retval.AACTION := c1.AACTION;

        pipe row (retval);

        prev_c1 := c1;
   
    end loop;
    
    return;

end;

function CALC_ACTIONS_TOTALS (p_cursor CURSOR_ACTIONS_T, p_lag integer, p_initial_balance number default 1000)
return CALC_ACTIONS_TOTALS_LIST_T
pipelined order p_cursor by (STOCK_NAME, ADATE) parallel_enable (partition p_cursor by hash (STOCK_NAME)) is
    l_deal_amount_of_buy       number;
    l_deal_stock                number;
    l_deal_amount               number;
    l_aaction_lag               varchar2(10);
    c1                          ACTION_T;
    prev_c1                     ACTION_T;
    retval                      CALC_ACTIONS_TOTALS_T;
    type ACTION_HISTORY_T is table of varchar2(16);
    l_aaction_history ACTION_HISTORY_T := ACTION_HISTORY_T();
    procedure f_sale (p_sale_price number) is
    begin
        if l_deal_stock > 0
        then
            l_deal_amount := - l_deal_amount_of_buy + round (l_deal_stock * p_sale_price, 2);
            l_deal_stock := 0;

            if    l_deal_amount > 0 then -- profit

                retval.DEALS_PROFIT_AMOUNT := retval.DEALS_PROFIT_AMOUNT + l_deal_amount;
                retval.DEALS_PROFIT_COUNT  := retval.DEALS_PROFIT_COUNT + 1;

            elsif l_deal_amount < 0 then -- loss

                retval.DEALS_LOSS_AMOUNT   := retval.DEALS_LOSS_AMOUNT   - l_deal_amount;
                retval.DEALS_LOSS_COUNT    := retval.DEALS_LOSS_COUNT + 1;

            end if;

            retval.BALANCE_RESULT  := retval.BALANCE_RESULT + l_deal_amount;
            retval.IN_STOCK := 'In stock';
        else
            retval.IN_STOCK := null;
        end if; 

    end; 
begin

    loop

        fetch p_cursor into c1;
        exit when p_cursor%notfound;
        
        if c1.STOCK_NAME is null or c1.ADATE is null or c1.APRICE is null
        then
            raise_application_error (-20001, 'Fields STOCK_NAME, ADATE, APRICE must be not null.');
        end if;
         
        if prev_c1.STOCK_NAME is not null
          and (   (c1.STOCK_NAME < prev_c1.STOCK_NAME)
               or (c1.STOCK_NAME = prev_c1.STOCK_NAME and c1.ADATE < prev_c1.ADATE))
        then
            raise_application_error (-20001, 'Rowset must be ordered by STOCK_NAME, ADATE.');
        end if;
        
        if c1.STOCK_NAME <> prev_c1.STOCK_NAME or prev_c1.STOCK_NAME is null
        then

            if prev_c1.STOCK_NAME is not null then
                f_sale (prev_c1.APRICE);
                pipe row (retval);
            end if;
            
            retval.BALANCE_RESULT := p_initial_balance;

            retval.DEALS_COUNT := 0;
            retval.STOCK_NAME := c1.STOCK_NAME;

            retval.DEALS_PROFIT_AMOUNT := 0;
            retval.DEALS_LOSS_AMOUNT   := 0;
            retval.DEALS_PROFIT_COUNT  := 0;
            retval.DEALS_LOSS_COUNT    := 0;
            l_aaction_history.delete;            
        end if;
    
        l_aaction_history.extend(1);
        l_aaction_history(l_aaction_history.last) := c1.AACTION;
  
        if l_aaction_history.last > p_lag
        then
            l_aaction_lag := l_aaction_history (l_aaction_history.last - p_lag);
            if     l_aaction_lag = 'BUY'
            then
                l_deal_stock        := floor (1000 * retval.BALANCE_RESULT / c1.APRICE) / 1000;
                l_deal_amount_of_buy := round (l_deal_stock * c1.APRICE, 2);
         
                retval.DEALS_COUNT := retval.DEALS_COUNT + 1;
                                
            elsif l_aaction_lag = 'SELL' 
            then
                f_sale (c1.APRICE);
            end if;
        end if;

        prev_c1 := c1;
   
    end loop;
    
    if prev_c1.STOCK_NAME is not null
    then
        f_sale (prev_c1.APRICE);
        pipe row (retval);
    end if;
    
    return;

end;


end;
/
