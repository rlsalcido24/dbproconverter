-------------------------------------------------------------------

set @StartTime = getdate()    

if @WithDebug = 1
begin
    select @exectime = 'Step #0: StartTime = ' + convert(char(23), getdate(), 121), @dt = getdate()
    print @exectime
end
----------------------------- DECLARE >>>
DECLARE 
    @unknown_CLIENT_ID      int,
    @unknown_PRODUCT_ID     int,
    @unknown_RATE_ID        int,
    @unknown_RATE_ID_CAD    int,
    @unknown_RATE_ID_USD    int,
    @unknown_SALESPERSON_ID int,
    @WarningCode 		    int,
	@WarningMessage 		varchar(255)

DECLARE @creation_dt datetime 	-- last update date and creation date MUST be same for new records, 
-- v.15.0 DECLARE @MAX_LAST_UPDATE_DATE datetime  
DECLARE @id int, @id_max int, @records int, @rowcount int

DECLARE @STRUC_RATE_PRODUCT_ID int -- v.2.0
--DECLARE @APPLICATION varchar(10), @APPLICATION_USE varchar(10)

----------------------------- DECLARE <<<

-- v.3.8.1 >>>
declare @START_DT_V2 as date
set @START_DT_V2 = '2015-03-15'
-- v.3.8.1 <<<



SELECT @ErrorCode = 0, @ErrorMessage = '', @WithWarning = 0, @WarningCode = 0, @WarningMessage = ''

IF NOT EXISTS(SELECT * FROM dbo.ETL_CONTROL_TABLE WHERE SOURCE_SYSTEM_CODE = @SOURCE_SYSTEM_CODE AND STATUS = 'Y')
	GOTO ExitSP
    
SET @EXTRACT_ID = ( 
    SELECT MAX(EXTRACT_ID) 
    FROM dbo.ETL_CONTROL_TABLE
    WHERE SOURCE_SYSTEM_CODE = @SOURCE_SYSTEM_CODE )
    
    
   
IF NOT EXISTS(SELECT * FROM dbo.ETL_CONTROL_TABLE 
    WHERE SOURCE_SYSTEM_CODE = @SOURCE_SYSTEM_CODE AND STATUS = 'Y'
        AND EXTRACT_ID = @EXTRACT_ID ) 
	BEGIN		 
		SELECT	@ErrorMessage = object_name(@@procid) + ': Execution rejected. Last record in ETL_CONTROL_TABLE table for SOURCE_SYSTEM_CODE = ''' + RTRIM(@SOURCE_SYSTEM_CODE) + ''' must be with STATUS = ''Y''.'

		SET @ErrorCode = -10
		
		GOTO ExitSP	
	END
    
IF (SELECT COUNT(*) FROM dbo.ETL_CONTROL_TABLE WHERE SOURCE_SYSTEM_CODE = @SOURCE_SYSTEM_CODE AND STATUS = 'Y') <> 1
BEGIN		 
	SELECT	@ErrorMessage = object_name(@@procid) + ': Execution rejected. Only one record must exists in ETL_CONTROL_TABLE table with STATUS = ''Y'' for SOURCE_SYSTEM_CODE = ''' + RTRIM(@SOURCE_SYSTEM_CODE) + '''.'

	SET @ErrorCode = -20
	
	GOTO ExitSP	
END

if @WithDebug = 1
begin
    select @exectime = 'Step #1: @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end

--v.13.4 >>>
--based on the source provider, change the landing table 
if OBJECT_ID('tempdb..##TDW_LND_DATA_PROCESSED') IS NOT NULL 
   begin
        drop table ##TDW_LND_DATA_PROCESSED
   end

if OBJECT_ID('tempdb..#TDW_LND_DATA_PROCESSED') IS NOT NULL 
   begin
        drop table #TDW_LND_DATA_PROCESSED
   end

if @SRC_PROVIDER = 'OTIS' 
  begin
     exec('select * into ##TDW_LND_DATA_PROCESSED from TDW_LND')
	 
	 alter table ##TDW_LND_DATA_PROCESSED add SRC_CPTY_LEGAL_ENTITY varchar(20) collate Latin1_General_BIN -- dummy column
	 alter table ##TDW_LND_DATA_PROCESSED add SRC_COMPETITIVE_FLAG varchar(1) collate Latin1_General_BIN null  
	 alter table ##TDW_LND_DATA_PROCESSED add SRC_PRE_NUM_DEALERS [int]  NULL
	 alter table ##TDW_LND_DATA_PROCESSED add IS_PROCESSED_TRADE varchar(1) collate Latin1_General_BIN not null default 'N'
		alter table ##TDW_LND_DATA_PROCESSED add MESSAGE_ID  varchar(100) collate Latin1_General_BIN NULL 
		alter table ##TDW_LND_DATA_PROCESSED add SRC_GTM_TRADE_ID    varchar(50) collate Latin1_General_BIN NULL 
	 if @WithDebug = 1
	 begin
		select @exectime = 'Step #1.0.1 OTIS: @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
		print @exectime
	 end
  end
else if @SRC_PROVIDER = 'LAKE' 
  begin
     exec('select * into ##TDW_LND_DATA_PROCESSED from TDW_LND_DATALAKE_PROCESSED')
	 if @WithDebug = 1
	 begin
		select @exectime = 'Step #1.0.1 LAKE: @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
		print @exectime
	 end

  end

select * into #TDW_LND_DATA_PROCESSED from ##TDW_LND_DATA_PROCESSED

if (select count(*) from #TDW_LND_DATA_PROCESSED) = 0
begin
	select	@ErrorMessage = object_name(@@procid) + ': Execution failed. There are NO records in Landing table for SOURCE_SYSTEM_CODE = ''' + RTRIM(@SOURCE_SYSTEM_CODE) + '''.'
	SET @ErrorCode = -20	
	GOTO ExitSP	
end

--v.13.4 <<<


-- v.12.8 >>>
select RSS_PRODUCT_ID, CC_CODE, START_DT, END_DT, MULTIPLIER
    into #SALES_GRID_ADJUSTMENT 
from (
    select RSS_PRODUCT_ID, CC_CODE, START_DT, END_DT, MULTIPLIER 
    from SALES_GRID_ADJUSTMENT 
    where SRC_SYSTEM_CD = @SRC_SYSTEM_CD
        and WITH_CHILDREN = 'N'    
    ) t;
-- v.7.0 >>>    
insert into #SALES_GRID_ADJUSTMENT(RSS_PRODUCT_ID, CC_CODE, START_DT, END_DT, MULTIPLIER)
    select t2.RSS_PRODUCT_ID, t1.CC_CODE, t1.START_DT, t1.END_DT, t1.MULTIPLIER
    from SALES_GRID_ADJUSTMENT t1
        inner join RSS_PRODUCT t2 on
            t1.RSS_PRODUCT_ID in (t2.RSS_PRODUCT_LVL1_ID, t2.RSS_PRODUCT_LVL2_ID, t2.RSS_PRODUCT_LVL3_ID, t2.RSS_PRODUCT_LVL4_ID, t2.RSS_PRODUCT_LVL5_ID)
        inner join RSS_PRODUCT t3 on
            t1.RSS_PRODUCT_ID = t3.RSS_PRODUCT_ID
        inner join (
            select t02.RSS_PRODUCT_ID, CC_CODE, START_DT, END_DT, max(t03.PRODUCT_HIERARCHY_DEPTH) as PRODUCT_HIERARCHY_DEPTH_MAX 
            from SALES_GRID_ADJUSTMENT t01
                inner join RSS_PRODUCT t02 on
                    t01.RSS_PRODUCT_ID in (t02.RSS_PRODUCT_LVL1_ID, t02.RSS_PRODUCT_LVL2_ID, t02.RSS_PRODUCT_LVL3_ID, t02.RSS_PRODUCT_LVL4_ID, t02.RSS_PRODUCT_LVL5_ID)
                inner join RSS_PRODUCT t03 on
                    t01.RSS_PRODUCT_ID = t03.RSS_PRODUCT_ID     
            where t01.SRC_SYSTEM_CD = @SRC_SYSTEM_CD
                and t01.WITH_CHILDREN = 'Y'
            group by t02.RSS_PRODUCT_ID, CC_CODE, START_DT, END_DT    
        ) t4 on
        t2.RSS_PRODUCT_ID = t4.RSS_PRODUCT_ID
            and t1.CC_CODE = t4.CC_CODE
            and t1.START_DT = t4.START_DT
            and t1.END_DT = t4.END_DT
            and t3.PRODUCT_HIERARCHY_DEPTH = t4.PRODUCT_HIERARCHY_DEPTH_MAX         
    where t1.SRC_SYSTEM_CD = @SRC_SYSTEM_CD
        and t1.WITH_CHILDREN = 'Y' 
        and not exists(select * from #SALES_GRID_ADJUSTMENT where RSS_PRODUCT_ID = t2.RSS_PRODUCT_ID and CC_CODE = t1.CC_CODE and START_DT = t1.START_DT and END_DT = t1.END_DT )   
-- v.7.0 <<<    

-- v.8.0 >>>
select RSS_PRODUCT_ID, CC_CODE, START_DT, END_DT, MULTIPLIER
    into #SALES_GRID_ADJUSTMENT_PRE_REPRICING 
from (
    select RSS_PRODUCT_ID, CC_CODE, START_DT, END_DT, MULTIPLIER 
    from SALES_GRID_ADJUSTMENT_PRE_REPRICING 
    where SRC_SYSTEM_CD = @SRC_SYSTEM_CD
        and WITH_CHILDREN = 'N'  
    ) t;
   
insert into #SALES_GRID_ADJUSTMENT_PRE_REPRICING(RSS_PRODUCT_ID, CC_CODE, START_DT, END_DT, MULTIPLIER)
    select t2.RSS_PRODUCT_ID, t1.CC_CODE, t1.START_DT, t1.END_DT, t1.MULTIPLIER
    from SALES_GRID_ADJUSTMENT_PRE_REPRICING t1
        inner join RSS_PRODUCT t2 on
            t1.RSS_PRODUCT_ID in (t2.RSS_PRODUCT_LVL1_ID, t2.RSS_PRODUCT_LVL2_ID, t2.RSS_PRODUCT_LVL3_ID, t2.RSS_PRODUCT_LVL4_ID, t2.RSS_PRODUCT_LVL5_ID)
        inner join RSS_PRODUCT t3 on
            t1.RSS_PRODUCT_ID = t3.RSS_PRODUCT_ID
        inner join (
            select t02.RSS_PRODUCT_ID, CC_CODE, START_DT, END_DT, max(t03.PRODUCT_HIERARCHY_DEPTH) as PRODUCT_HIERARCHY_DEPTH_MAX 
            from SALES_GRID_ADJUSTMENT_PRE_REPRICING t01
                inner join RSS_PRODUCT t02 on
                    t01.RSS_PRODUCT_ID in (t02.RSS_PRODUCT_LVL1_ID, t02.RSS_PRODUCT_LVL2_ID, t02.RSS_PRODUCT_LVL3_ID, t02.RSS_PRODUCT_LVL4_ID, t02.RSS_PRODUCT_LVL5_ID)
                inner join RSS_PRODUCT t03 on
                    t01.RSS_PRODUCT_ID = t03.RSS_PRODUCT_ID     
            where t01.SRC_SYSTEM_CD = @SRC_SYSTEM_CD
                and t01.WITH_CHILDREN = 'Y'
            group by t02.RSS_PRODUCT_ID, CC_CODE, START_DT, END_DT    
        ) t4 on
        t2.RSS_PRODUCT_ID = t4.RSS_PRODUCT_ID
            and t1.CC_CODE = t4.CC_CODE
            and t1.START_DT = t4.START_DT
            and t1.END_DT = t4.END_DT
            and t3.PRODUCT_HIERARCHY_DEPTH = t4.PRODUCT_HIERARCHY_DEPTH_MAX         
    where t1.SRC_SYSTEM_CD = @SRC_SYSTEM_CD
        and t1.WITH_CHILDREN = 'Y' 
        and not exists(select * from #SALES_GRID_ADJUSTMENT_PRE_REPRICING where RSS_PRODUCT_ID = t2.RSS_PRODUCT_ID and CC_CODE = t1.CC_CODE and START_DT = t1.START_DT and END_DT = t1.END_DT )   
   
-- v.8.0 <<<

-- v.4.3 >>>
select RSS_PRODUCT_ID, START_DT, END_DT, SRC_RISK_CD, MULTIPLIER 
    into #SALES_GRID_RISK_CODE_PRODUCT 
from ( 
    select RSS_PRODUCT_ID, START_DT, END_DT, SRC_RISK_CD, MULTIPLIER 
    from SALES_GRID_RISK_CODE_PRODUCT 
    where SRC_SYSTEM_CD = @SRC_SYSTEM_CD 
        and WITH_CHILDREN = 'N' 
    union 
    select t2.RSS_PRODUCT_ID,  START_DT, END_DT, SRC_RISK_CD, MULTIPLIER 
    from SALES_GRID_RISK_CODE_PRODUCT t1 
        inner join RSS_PRODUCT t2 on             
t1.RSS_PRODUCT_ID in (t2.RSS_PRODUCT_LVL1_ID, t2.RSS_PRODUCT_LVL2_ID, t2.RSS_PRODUCT_LVL3_ID, t2.RSS_PRODUCT_LVL4_ID, t2.RSS_PRODUCT_LVL5_ID) 
    where t1.SRC_SYSTEM_CD = @SRC_SYSTEM_CD 
        and t1.WITH_CHILDREN = 'Y' 
    ) t 
-- v.4.3 <<<

-- v.5.1 >>>
select RSS_PRODUCT_ID, START_DT, END_DT, SRC_CCY_CD, SRC_RISK_CD, MULTIPLIER 
    into #SALES_GRID_RISK_CODE_PRODUCT_CCY 
from ( 
    select RSS_PRODUCT_ID, START_DT, END_DT, SRC_CCY_CD, SRC_RISK_CD, MULTIPLIER 
    from SALES_GRID_RISK_CODE_PRODUCT_CCY 
    where SRC_SYSTEM_CD = @SRC_SYSTEM_CD 
        and WITH_CHILDREN = 'N' 
    union 
    select t2.RSS_PRODUCT_ID,  START_DT, END_DT, SRC_CCY_CD, SRC_RISK_CD, MULTIPLIER 
    from SALES_GRID_RISK_CODE_PRODUCT_CCY t1 
        inner join RSS_PRODUCT t2 on             
t1.RSS_PRODUCT_ID in (t2.RSS_PRODUCT_LVL1_ID, t2.RSS_PRODUCT_LVL2_ID, t2.RSS_PRODUCT_LVL3_ID, t2.RSS_PRODUCT_LVL4_ID, t2.RSS_PRODUCT_LVL5_ID) 
    where t1.SRC_SYSTEM_CD = @SRC_SYSTEM_CD 
        and t1.WITH_CHILDREN = 'Y' 
    ) t 
-- v.5.1 <<<

-- v.8.0 >>>
-- drop table #SALES_GRID_RISK_CODE_PRODUCT_CCY_PRE_REPRICING declare @SRC_SYSTEM_CD varchar(20) set @SRC_SYSTEM_CD = 'TOME'
select RSS_PRODUCT_ID, START_DT, END_DT, SRC_CCY_CD, SRC_RISK_CD, MULTIPLIER 
    into #SALES_GRID_RISK_CODE_PRODUCT_CCY_PRE_REPRICING 
from ( 
    select RSS_PRODUCT_ID, START_DT, END_DT, SRC_CCY_CD, SRC_RISK_CD, MULTIPLIER 
    from SALES_GRID_RISK_CODE_PRODUCT_CCY_PRE_REPRICING 
    where SRC_SYSTEM_CD = @SRC_SYSTEM_CD 
        and WITH_CHILDREN = 'N' 
    union 
    select t2.RSS_PRODUCT_ID,  START_DT, END_DT, SRC_CCY_CD, SRC_RISK_CD, MULTIPLIER 
    from SALES_GRID_RISK_CODE_PRODUCT_CCY_PRE_REPRICING t1 
        inner join RSS_PRODUCT t2 on             
t1.RSS_PRODUCT_ID in (t2.RSS_PRODUCT_LVL1_ID, t2.RSS_PRODUCT_LVL2_ID, t2.RSS_PRODUCT_LVL3_ID, t2.RSS_PRODUCT_LVL4_ID, t2.RSS_PRODUCT_LVL5_ID) 
    where t1.SRC_SYSTEM_CD = @SRC_SYSTEM_CD 
        and t1.WITH_CHILDREN = 'Y' 
    ) t
-- select * from #SALES_GRID_RISK_CODE_PRODUCT_CCY_PRE_REPRICING      
-- v.8.0 <<<


-- v.5.4 >>>
select RSS_PRODUCT_ID, START_DT, END_DT, SRC_CCY_CD, SRC_RISK_CD, ETRADE_FLAG, MULTIPLIER 
    into #SALES_GRID_RISK_CODE_PRODUCT_CCY_CHANNEL 
from ( 
    select RSS_PRODUCT_ID, START_DT, END_DT, SRC_CCY_CD, SRC_RISK_CD, ETRADE_FLAG, MULTIPLIER 
    from SALES_GRID_RISK_CODE_PRODUCT_CCY_CHANNEL 
    where SRC_SYSTEM_CD = @SRC_SYSTEM_CD 
        and WITH_CHILDREN = 'N' 
    union 
    select t2.RSS_PRODUCT_ID,  START_DT, END_DT, SRC_CCY_CD, SRC_RISK_CD, ETRADE_FLAG, MULTIPLIER 
    from SALES_GRID_RISK_CODE_PRODUCT_CCY_CHANNEL t1 
        inner join RSS_PRODUCT t2 on             
t1.RSS_PRODUCT_ID in (t2.RSS_PRODUCT_LVL1_ID, t2.RSS_PRODUCT_LVL2_ID, t2.RSS_PRODUCT_LVL3_ID, t2.RSS_PRODUCT_LVL4_ID, t2.RSS_PRODUCT_LVL5_ID) 
    where t1.SRC_SYSTEM_CD = @SRC_SYSTEM_CD 
        and t1.WITH_CHILDREN = 'Y' 
    ) t 
-- v.5.4 <<<


-- LOG
DELETE ETL_CONTROL_TABLE_LOG
WHERE EXTRACT_ID = @EXTRACT_ID

if @WithDebug = 1
begin
    select @exectime = 'Step #1.1: @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end

INSERT INTO ETL_CONTROL_TABLE_LOG (
    EXTRACT_ID,
    Type,   
    Code,     
    Message )
SELECT 
    @EXTRACT_ID AS EXTRACT_ID,
    1 AS Type,       
    -10000 AS Code,
    'Unexpected Error' as Message

if @WithDebug = 1
begin
    select @exectime = 'Step #1.2: @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end

-- v.3.6 >>>
declare @unknown_RSS_SECURITY_ID int
select @unknown_RSS_SECURITY_ID = MIN(RSS_SECURITY_ID) 
from dbo.SRDR_SECURITIES_STG 
where SECURITY_NAME = 'UNKNOWN'

IF @unknown_RSS_SECURITY_ID IS NULL
BEGIN    		 
    SET	@ErrorMessage = object_name(@@procid) + ': Cannot specify  RSS_SECURITY_ID for unknown security name from SRDR_SECURITIES_STG table.'
    SET @ErrorCode = -25
    GOTO ExitSP
END	
    
if @WithDebug = 1
begin
    select @exectime = 'Step #1.3: @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end
-- v.3.6 <<<

set rowcount 1000000;
-- declare @rowcount int = 1, @WithDebug bit = 1, @exectime varchar(500), @dt datetime = getdate()
set @rowcount = 1
while @rowcount > 0
begin
    DELETE t1
    -- select count(*)
    FROM RSS_TRADE_STG t1 
	    INNER JOIN #TDW_LND_DATA_PROCESSED t2 ON
            t1.SRC_SYSTEM_CD = t2.SRC_SYSTEM_CD -- v.8.1
                and t1.RSS_TRADE_ID = t2.RSS_TRADE_SK
			    AND t1.START_DT = t2.SRC_LAST_UPDATE_DT
    WHERE t1.SRC_SYSTEM_CD = 'TOME' 
        and t2.ERROR_CD = '0' -- v.8.1
		and isnull(t2.SRC_PROVIDER,'') <> 'POTS'		--csip-9563

    SELECT @ErrorCode = @@ERROR, @rowcount = @@ROWCOUNT

    IF @ErrorCode <> 0
	BEGIN		 
		SET	@ErrorMessage = object_name(@@procid) + ': SQL Server Error #'+cast(@ErrorCode as varchar(10))+' occured while deleting data from RSS_TRADE_STG table.'
		SET @ErrorCode = -30
	    set rowcount 0;
		GOTO ExitSP	
	END

    if @WithDebug = 1
    begin
        select @exectime = 'Step #2.1: Deleted '+ cast(@rowcount as varchar(10)) +' rows. @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
        print @exectime
    end
    -- select @rowcount, getdate() 
end
set rowcount 0;
-- v.12.2 <<<

--select top 0 * into #TDW_LND from TDW_LND
--drop table #TDW_LND
-- v.8.1 >>>
DELETE RSS_TRADE_STG
-- select count(*) 
-- select t1.RSS_TRADE_ID, t1.START_DT, t1.END_DT, t1.SRC_TRADE_DT, t1.IS_CURRENT_IND
FROM RSS_TRADE_STG t1 
	--INNER JOIN TDW_LND t2 ON --v.13.14
	INNER JOIN #TDW_LND_DATA_PROCESSED t2 ON --v.13.4
        t1.SRC_SYSTEM_CD = t2.SRC_SYSTEM_CD
            and t1.RSS_TRADE_ID = t2.RSS_TRADE_SK
			AND t1.START_DT > t2.SRC_LAST_UPDATE_DT
WHERE t1.SRC_SYSTEM_CD = 'TOME' -- @SRC_SYSTEM_CD
    and t2.ERROR_CD = '0'  
	and isnull(t2.SRC_PROVIDER,'') <> 'POTS'		--csip-9563	
	

SELECT @ErrorCode = @@ERROR, @rowcount = @@ROWCOUNT

IF @ErrorCode <> 0
	BEGIN		 
		SET	@ErrorMessage = object_name(@@procid) + ': SQL Server Error #'+cast(@ErrorCode as varchar(10))+' occured while deleting data from RSS_TRADE_STG table.'
		SET @ErrorCode = -30
	
		GOTO ExitSP	
	END
    
if @WithDebug = 1
begin
    select @exectime = 'Step #2.3: Deleted '+ cast(@rowcount as varchar(10)) +' rows. @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end
-- v.8.1 <<<

SET @creation_dt = getdate()	

-- v.2.0 >>>
select @STRUC_RATE_PRODUCT_ID = RSS_PRODUCT_ID 
from RSS_PRODUCT 
where PRODUCT_NM = 'Structured Rates'

IF @STRUC_RATE_PRODUCT_ID IS NULL
	BEGIN		 
		SET	@ErrorMessage = object_name(@@procid) + ': Cannot specify PRODUCT_ID for ''Structured Rates'' value from RSS_PRODUCT table.'
		SET @ErrorCode = -39
		
		GOTO ExitSP	
	END

select distinct SRC_BOOK_ID
    into #tBDR_BOOK
from BDR_BOOK_STG
where TRANSIT_NUMBER in (
    select TRANSIT_NUMBER
    from BDR_TRANSIT
    where MANAGEMENT_ID in (
        select MANAGEMENT_ID from BDR_MANAGEMENT_TREE
        where PARENT_MANAGEMENT_ID in( 
        select MANAGEMENT_ID from BDR_MANAGEMENT
        -- v.2.7 where ROLLUP_LEVEL = 6
        where ROLLUP_LEVEL = 7 -- v.2.7
        and MANAGEMENT_ID in (81311, 81316))
    )
)
    and SRC_SYSTEM_CD = 'TOME'
    and SRC_BOOK_ID not like 'NAGY%' -- v.2.1

-- select * from #tBDR_BOOK
create /*clustered*/ index IX01_#tBDR_BOOK on #tBDR_BOOK(SRC_BOOK_ID)

if @WithDebug = 1
begin
    select @exectime = 'Step #5.0 #tBDR_BOOK: @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end

-- v.2.0 <<<   

-- @unknown_PRODUCT_ID
SELECT @unknown_PRODUCT_ID = MIN(RSS_PRODUCT_ID)
FROM RSS_PRODUCT 
WHERE UPPER(PRODUCT_NM) LIKE '%UNKNOWN%'  

IF @unknown_PRODUCT_ID IS NULL
	BEGIN		 
		SET	@ErrorMessage = object_name(@@procid) + ': Cannot specify PRODUCT_ID for ''UNKNOWN'' value from RSS_PRODUCT table.'
		SET @ErrorCode = -50
		
		GOTO ExitSP	
	END

-- @unknown_RATE_ID
SELECT @unknown_RATE_ID = FX_RATE_ID
FROM FX_RATE
WHERE FROM_CURRENCY_CODE = 'RSS' AND TO_CURRENCY_CODE = 'RSS' AND FX_RATE_DATE = '1900-01-01'

IF @unknown_RATE_ID IS NULL
	BEGIN		 
		SET	@ErrorMessage = object_name(@@procid) + ': Cannot specify RATE_ID for ''UNKNOWN'' value from FX_RATE table.'
		SET @ErrorCode = -60
		
		GOTO ExitSP	
	END
    
SELECT TOP 1 @unknown_RATE_ID_CAD = FX_RATE_ID
FROM FX_RATE
WHERE FROM_CURRENCY_CODE = 'CAD' AND TO_CURRENCY_CODE = 'CAD'

IF @unknown_RATE_ID_CAD IS NULL
	BEGIN
        INSERT INTO FX_RATE(            
            FX_RATE, 
            FROM_CURRENCY_CODE, 
            TO_CURRENCY_CODE, 
            PRODUCER_LAST_UPDATE_ID,
            ERROR_CD,
            FX_RATE_DATE,
            SRC_LAST_UPDATE_DT, 
            STG_CREATED_BY_UID, 
            STG_CREATION_DT, 
            STG_LAST_UPDATE_UID, 
            STG_LAST_UPDATE_DT )
        SELECT             
            1.0,           -- FX_RATE
            'CAD',       -- FROM_CURRENCY_CODE 
            'CAD',       -- TO_CURRENCY_CODE
            'RSS',       -- PRODUCER_LAST_UPDATE_ID 
            '0',         -- ERROR_CD
            '1900-01-01',   -- FX_RATE_DATE,
            GETDATE(),   -- SRC_LAST_UPDATE_DT, 
            USER_NAME(), -- STG_CREATED_BY_UID 
            GETDATE(),   -- STG_CREATION_DT 
            USER_NAME(), -- STG_LAST_UPDATE_UID
            GETDATE()    -- STG_LAST_UPDATE_DT
            
        SELECT @ErrorCode = @@ERROR

        IF @ErrorCode <> 0
	        BEGIN		 
		        SET	@ErrorMessage = object_name(@@procid) + ': SQL Server Error #'+cast(@ErrorCode as varchar(10))+' occured while inserting FROM_CURRENCY_CODE = ''CAD'' AND TO_CURRENCY_CODE = ''CAD'' value into FX_RATE table.'
		        SET @ErrorCode = -70
		
		        GOTO ExitSP	
	        END
            
        SELECT TOP 1 @unknown_RATE_ID_CAD = FX_RATE_ID
        FROM FX_RATE
        WHERE FROM_CURRENCY_CODE = 'CAD' AND TO_CURRENCY_CODE = 'CAD'
        
        IF @unknown_RATE_ID_CAD IS NULL
            BEGIN    		 
		        SET	@ErrorMessage = object_name(@@procid) + ': Cannot specify RATE_ID for FROM_CURRENCY_CODE = ''CAD'' AND TO_CURRENCY_CODE = ''CAD'' values from FX_RATE table.'
		        SET @ErrorCode = -75
		
		        GOTO ExitSP
            END	
	END

--Check for default USD Conversion
SELECT TOP 1 @unknown_RATE_ID_USD = FX_RATE_ID
FROM FX_RATE
WHERE FROM_CURRENCY_CODE = 'USD' AND TO_CURRENCY_CODE = 'USD'

-- If default USD to USD conversion is not in FX_RATE table, add it.
IF @unknown_RATE_ID_USD IS NULL
	BEGIN
        INSERT INTO FX_RATE(            
            FX_RATE, 
            FROM_CURRENCY_CODE, 
            TO_CURRENCY_CODE, 
            PRODUCER_LAST_UPDATE_ID,
            ERROR_CD,
            FX_RATE_DATE,
            SRC_LAST_UPDATE_DT, 
            STG_CREATED_BY_UID, 
            STG_CREATION_DT, 
            STG_LAST_UPDATE_UID, 
            STG_LAST_UPDATE_DT )
        SELECT             
            1.0,           -- FX_RATE
            'USD',       -- FROM_CURRENCY_CODE 
            'USD',       -- TO_CURRENCY_CODE
            'RSS',       -- PRODUCER_LAST_UPDATE_ID 
            '0',         -- ERROR_CD
            '1900-01-01',   -- FX_RATE_DATE,
            GETDATE(),   -- SRC_LAST_UPDATE_DT, 
            USER_NAME(), -- STG_CREATED_BY_UID 
            GETDATE(),   -- STG_CREATION_DT 
            USER_NAME(), -- STG_LAST_UPDATE_UID
            GETDATE()    -- STG_LAST_UPDATE_DT
            
        SELECT @ErrorCode = @@ERROR

        IF @ErrorCode <> 0
	        BEGIN		 
		        SET	@ErrorMessage = object_name(@@procid) + ': SQL Server Error #'+cast(@ErrorCode as varchar(10))+' occured while inserting FROM_CURRENCY_CODE = ''USD'' AND TO_CURRENCY_CODE = ''USD'' value into FX_RATE table.'
		        SET @ErrorCode = -70
		
		        GOTO ExitSP	
	        END
            
        SELECT TOP 1 @unknown_RATE_ID_USD = FX_RATE_ID
        FROM FX_RATE
        WHERE FROM_CURRENCY_CODE = 'USD' AND TO_CURRENCY_CODE = 'USD'
        
        IF @unknown_RATE_ID_USD IS NULL
            BEGIN    		 
		        SET	@ErrorMessage = object_name(@@procid) + ': Cannot specify RATE_ID for FROM_CURRENCY_CODE = ''USD'' AND TO_CURRENCY_CODE = ''USD'' values from FX_RATE table.'
		        SET @ErrorCode = -75
		
		        GOTO ExitSP
            END	
	END

-- @unknown_SALESPERSON_ID
SELECT @unknown_SALESPERSON_ID = RSS_SALES_PERSON_ID 
FROM RSS_SALESPERSON_KEY 
WHERE SALES_PERSON_EMAIL_ADDSS = 'UNKNOWN'

IF @unknown_SALESPERSON_ID IS NULL
	BEGIN		 
		SET	@ErrorMessage = object_name(@@procid) + ': Cannot specify SALESPERSON_ID for ''UNKNOWN'' value from RSS_SALESPERSON_KEY table.'
		SET @ErrorCode = -80
		
		GOTO ExitSP	
	END
    
if @WithDebug = 1
begin
    select @exectime = 'Step #3: @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end

-- v.9.1 >>>
-- drop table #SRC_V_TRADE_IND
----CSIP-9774
select distinct SRC_E_TRADE_IND, PROCESSED_TRADE
    into #SRC_V_TRADE_IND
from RSS_ECHANNEL
where SRC_SYSTEM_CD = 'TOME'
    and E_TRADE_IND_CATEGORY in ('Voice') 

-- v.10.1 >>>
if not exists(select * from #SRC_V_TRADE_IND where SRC_E_TRADE_IND = 'VOIX' )   
    insert into #SRC_V_TRADE_IND(SRC_E_TRADE_IND, PROCESSED_TRADE)
        select 'VOIX' as SRC_E_TRADE_IND, NULL AS PROCESSED_TRADE  -----CSIP-9774
-- v.10.1 <<<

update  #SRC_V_TRADE_IND set PROCESSED_TRADE =NULL

-- v.3.3 >>>   
    insert into #SRC_V_TRADE_IND(SRC_E_TRADE_IND, PROCESSED_TRADE)
        select distinct SRC_E_TRADE_IND, PROCESSED_TRADE  
		from RSS_ECHANNEL
		where E_TRADE_IND_CATEGORY in ('Electronic (Sales)', 'Electronic (Trade Desk)' )  and PROCESSED_TRADE = 'Y'
		and SRC_E_TRADE_IND not in ('DX', 'RBCDX') -- v.4.2
		and SRC_SYSTEM_CD = 'TOME' -----CSIP-9774

---- drop table #SRC_E_TRADE_IND
select distinct SRC_E_TRADE_IND, 'N' AS  PROCESSED_TRADE
    into #SRC_E_TRADE_IND
from RSS_ECHANNEL
where E_TRADE_IND_CATEGORY in ('Electronic (Sales)', 'Electronic (Trade Desk)' ) 
and SRC_E_TRADE_IND not in ('DX', 'RBCDX') -- v.4.2
and SRC_SYSTEM_CD = 'TOME' -----CSIP-9774

-----  IF not picked as the PROCESS_TRADE channel, it will be electronic even IS_PROCESS_TRADE is Y
    insert into #SRC_E_TRADE_IND(SRC_E_TRADE_IND, PROCESSED_TRADE)
        select distinct SRC_E_TRADE_IND, 'Y' AS PROCESSED_TRADE  
		from RSS_ECHANNEL
		where E_TRADE_IND_CATEGORY in ('Electronic (Sales)', 'Electronic (Trade Desk)' )  and PROCESSED_TRADE = 'N'
		and SRC_E_TRADE_IND not in ('DX', 'RBCDX') -- v.4.2
		and SRC_SYSTEM_CD = 'TOME' -----CSIP-9774



-- drop table #t1 declare @SRC_SYSTEM_CD varchar(20) set @SRC_SYSTEM_CD = 'TOME'
SELECT SRC_SYS_PRODUCT_ID, RSS_PRODUCT_ID
    INTO #t1
FROM  PRODUCT_RSS_2_SRC_SYS_XREF
WHERE  SRC_SYSTEM_CD = @SRC_SYSTEM_CD   

if @WithDebug = 1
begin
    select @exectime = 'Step #3.1: @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end

create index IX01_#t1 on #t1(SRC_SYS_PRODUCT_ID) -- v.12.2
-- v.3.3 <<<

-- v.8.1 >>>
-- drop table  #TDW_LND_0 declare @SRC_SYSTEM_CD varchar(20) = 'TOME'
select *,
    cast(SRC_BOOK_ID as varchar(40)) as SRC_PRODUCT_ID, cast(NULL as varchar(50)) as MAPPING_TYPE, cast(1 as int) as RSS_PRODUCT_ID, -- v.12.2
	' ' AS TRADE_IND_CATEGORY   -----CSIP-9774   E as electronic, V as Voice 
    into #TDW_LND_0
--from TDW_LND --13.4
from #TDW_LND_DATA_PROCESSED --13.4
where ERROR_CD = '0'    and SRC_SYSTEM_CD = @SRC_SYSTEM_CD
-- select SRC_BOOK_ID, SRC_PRODUCT_ID, * from #TDW_LND_0
if @WithDebug = 1
begin
    select @exectime = 'Step #3.2: @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end

create index IX01_#TDW_LND_0 on #TDW_LND_0(SRC_TRADE_ID);

if @WithDebug = 1
begin
    select @exectime = 'Step #3.3: @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end

----------------------v.2.6----------------------
update #TDW_LND_0 
    set SRC_BOOK_ID = SRC_LONGNOTE_3 
WHERE /*SRC_SYSTEM_CD='TOME' 
    AND*/ SRC_BOOK_ID in ('HKNONCRE' , 'KFICCRRA', 'HFICTAIW', 'KFICTAIW', 'KT', 'HKCORPS1','NCREASA2')
    AND SRC_LONGNOTE_3 IN (select SRC_BOOK_ID from BDR_BOOK_STG where SRC_SYSTEM_CD='TOME');      
---------------------v.2.6------------------------
-- v.8.3 >>> 
update #TDW_LND_0 
    set 
        SRC_RISK_CD = isnull(SRC_RISK_CD, ''),
        SRC_CCY_CD = coalesce(SRC_INSTRUMENT_CCY, SRC_CCY_CD, '') ;
-- v.8.3 <<<

-- v.10.1 >>>
update #TDW_LND_0 
    set 
        SRC_E_TRADE_IND = 'VOIX'
where SRC_COMMENT like 'VOIX%'; -- SRC_NOTES
-- v.10.1 <<<

if @WithDebug = 1
begin
    select @exectime = 'Step #3.4: @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end

-- v.8.1 <<<

-- v.12.2 map RSS_PRODUCT_ID >>>
create index IX02_#TDW_LND_0 on #TDW_LND_0(RSS_TRADE_SK, SRC_LAST_UPDATE_DT);
create index IX03_#TDW_LND_0 on #TDW_LND_0(SRC_TRADE_ID);
create index IX04_#TDW_LND_0 on #TDW_LND_0(SRC_BOOK_ID);

--set statistics profile on;
UPDATE #TDW_LND_0
    SET	
        RSS_PRODUCT_ID = @STRUC_RATE_PRODUCT_ID,
        MAPPING_TYPE = 'BDR'
WHERE SRC_BOOK_ID in (SELECT SRC_BOOK_ID from #tBDR_BOOK);
--set statistics profile off;
-- 00:01 (7273 row(s) affected)
select @rowcount = @@rowcount
if @WithDebug = 1
begin
    select @exectime = 'Step #12.2.2: update #tPRODUCT_MAP BDR   '+ cast(@rowcount as varchar(10)) +' rows. @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end 

UPDATE #TDW_LND_0
    SET	        
	    RSS_PRODUCT_ID  = t1.RSS_PRODUCT_ID,
        SRC_PRODUCT_ID  = e.NOTE,
        MAPPING_TYPE    = 'NOTE'
FROM #TDW_LND_0 stg
    inner join ( 
        select t1.SRC_TRADE_ID, t1.NOTE
        from TRADE_EXCEPTION t1
            inner join (
                select SRC_TRADE_ID, max(SRC_LAST_UPDATE_DT) MAX_SRC_LAST_UPDATE_DT
                from TRADE_EXCEPTION 
                where SRC_SYSTEM_CD = 'TOME'
                    and EXCEPTION_TYPE= 'NEW ISSUE ZERO' 
                    and isnull(NOTE, '') <> ''
                group by SRC_TRADE_ID
                ) t2 on
                    t1.SRC_TRADE_ID = t2.SRC_TRADE_ID
                        and t1.SRC_LAST_UPDATE_DT = t2.MAX_SRC_LAST_UPDATE_DT 
        where t1.SRC_SYSTEM_CD = 'TOME'
            and t1.EXCEPTION_TYPE= 'NEW ISSUE ZERO' 
            and isnull(t1.NOTE, '') <> ''   
        ) e on 
            stg.SRC_TRADE_ID = e.SRC_TRADE_ID                    
    inner join  #t1 t1 ON
        e.NOTE = t1.SRC_SYS_PRODUCT_ID
WHERE stg.MAPPING_TYPE is null;

select @rowcount = @@rowcount
if @WithDebug = 1
begin
    select @exectime = 'Step #12.2.3: update #tPRODUCT_MAP NOTE   '+ cast(@rowcount as varchar(10)) +' rows. @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end 

UPDATE #TDW_LND_0
    SET        
	    RSS_PRODUCT_ID = t1.RSS_PRODUCT_ID
        -- declare @SRC_SYSTEM_CD varchar(20) = 'TOME' select count(*)
FROM #TDW_LND_0 stg
    INNER JOIN #t1 t1 ON
        stg.SRC_BOOK_ID = t1.SRC_SYS_PRODUCT_ID
WHERE stg.MAPPING_TYPE IS NULL 
-- 00:24 (11026724 row(s) affected)

select @rowcount = @@rowcount
if @WithDebug = 1
begin
    select @exectime = 'Step #12.2.4: update #TDW_LND_0 '+ cast(@rowcount as varchar(10)) +' rows. @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end
-- v.12.2 map RSS_PRODUCT_ID <<<

-- v.15.0 >>>
UPDATE stg
    SET        
	    RSS_PRODUCT_ID = t1.RSS_PRODUCT_ID
FROM #TDW_LND_0 stg
    INNER JOIN PRODUCT_RSS_2_SRC_SYS_XREF_BY_DATE t1 ON
        stg.SRC_BOOK_ID = t1.SRC_SYS_PRODUCT_ID 
            and stg.SRC_TRADE_DT >= t1.START_DT and stg.SRC_TRADE_DT < t1.END_DT
WHERE stg.MAPPING_TYPE IS NULL 
    and t1.SRC_SYSTEM_CD = 'TOME'
-- 00:24 (11026724 row(s) affected)

select @rowcount = @@rowcount
if @WithDebug = 1
begin
    select @exectime = 'Step #12.2.4 BY_DATE: update #TDW_LND_0 '+ cast(@rowcount as varchar(10)) +' rows. @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end
-- v.15.0 <<<


----CSIP-9774
UPDATE l
    SET TRADE_IND_CATEGORY = 'V'     
FROM #TDW_LND_0 l
join  #SRC_V_TRADE_IND v on l.SRC_E_TRADE_IND = v.SRC_E_TRADE_IND AND   ( l.IS_PROCESSED_TRADE = v.PROCESSED_TRADE OR v.PROCESSED_TRADE is null) 

UPDATE l
    SET TRADE_IND_CATEGORY = 'E'     
FROM #TDW_LND_0 l
join  #SRC_E_TRADE_IND e on l.SRC_E_TRADE_IND = e.SRC_E_TRADE_IND AND    l.IS_PROCESSED_TRADE= e.PROCESSED_TRADE 
---<<CSIP-9774


select top 0 
    id = identity(int),
    RSS_TRADE_SK,
    SRC_BOOK_ID,
--v.1.10    SRC_CCY_CD,
    -- isnull(SRC_INSTRUMENT_CCY,SRC_CCY_CD) as SRC_CCY_CD,     --v.1.10
    SRC_CCY_CD, -- v.8.3
    SRC_CCY_CD as SRC_SETTLEMENT_CCY,      --v.1.10
    SRC_CUSIP,
    SRC_ISIN,
    SRC_E_TRADE_IND,
    EXTRACT_DT,
    SRC_ISSUE_PVBP,
    SRC_MATURITY_DT,
    SRC_SETTLEMENT_DT,
    SRC_ASSIGNED_TO_SALESPERSON_ID,
    SRC_CLIENT_ID,
    SRC_CLIENT_NM,
    SRC_COVERAGE_SALESPERSON_ID,
    SRC_ENTERED_BY_SALESPERSON_ID,
    SRC_LAST_UPDATE_DT,
    --SRC_PRODUCT_CD, v.13.2
    SRC_REVENUE_GRID_AMT,
    SRC_REVENUE_SP_MARKUP_AMT,
    SRC_SECURITY_ID,
    SRC_SECURITY_NM,
    SRC_SUB_ACCOUNT_NM,
    SRC_SYSTEM_CD,
    SRC_TRADE_ID,
    SRC_TRADE_STATUS_CD,
    SRC_TRADE_TYPE,
    SRC_TRADE_DT,
    SRC_TRADE_PRICE_AMT,
    SRC_TRADE_PROCEEDS_AMT,
    SRC_TRADE_QTY,
    SRC_TRADE_YIELD,
    SRC_TRADER_VALUE,
    SRC_LEGAL_ENTITY,
    SRC_RISK_CD,
    ERROR_CD,
    SRC_TRADE_SUBTYPE,   --v.1.2
    SRC_TRADER_ID, --v.1.6
    SRC_COMMENT,   --v.1.6
    SRC_ORIG_TRADE_SRC, --v.1.6
    SRC_ALLOCATION_COUNT, --v.1.6
    SRC_LONGNOTE_3,  -- v.2.6
    cast(0.0 as numeric(20,6)) as REVENUE_GRID_AMT_ORIG, -- v.3.5
    SRC_LONGNOTE_2, -- v.3.6
    cast(NULL as varchar(400)) as GRID_INFO, -- v.4.1    
    SRC_NATIVE_BOOK_ID, --v.5.0
    SRC_LONGNOTE_4, --v.5.2
    SRC_LONGNOTE_8, --v.6.0
    SRC_NUM_DEALERS, --v.6.0
    -- v.5.6 >>>
    cast(1.0 as numeric(20,6)) as HAIRCUT_MULTIPLIER,
    cast(0.0 as numeric(20,6)) as HAIRCUT_VALUE,
    cast(1.0 as numeric(20,6)) as POSITION_MULTIPLIER,
    cast(0.0 as numeric(20,6)) as POSITION_VALUE,   
    -- v.5.6 <<<
    SRC_ORDER_ID, -- v.6.6
    -- v,8.0 >>>
    cast(0.0 as numeric(20,6)) as PRE_REPRICING_ADJ_SRC_REVENUE_GRID_AMT,
    cast(NULL as varchar(400)) as PRE_REPRICING_ADJ_GRID_INFO,
    cast(0.0 as numeric(20,6)) as PRE_REPRICING_ADJ_SRC_REVENUE_SP_MARKUP_AMT,
    cast(0.0 as numeric(20,6)) as PRE_REPRICING_ADJ_SRC_REVENUE_NI_AMT,
	cast(0.0 as numeric(20,6)) as PRE_REPRICING_ADJ_SRC_TRADER_VALUE,
	SRC_REVENUE_BROKER_AMT,			--csip-4883
    -- v.8.0 <<<
    -- v.8.3 >>>
    cast(0.0 as numeric(20,6)) as GRID_AFTER_HAIRCUT,
    cast(1.0 as numeric(20,6)) as OTHER_HAIRCUT_MULTIPLIER,
    cast(1.0 as numeric(20,6)) as RISK_CODE_MULTIPLIER_NOT_AXE,
    cast(1.0 as numeric(20,6)) as RISK_CODE_MULTIPLIER_AXE,
    cast(1.0 as numeric(20,6)) as POSITION_MULTIPLIER_AXE,
    -- v.8.3 <<<
	SRC_CDR_CLIENT_ID, --v.10.2
	SRC_PROVIDER, --v.10.2
	SRC_TRADE_ID_ORIG as SRC_TRANS_ID, --v.10.3
    cast(SRC_BOOK_ID as varchar(40)) as SRC_PRODUCT_ID, cast(NULL as varchar(50)) as MAPPING_TYPE, cast(1 as int) as RSS_PRODUCT_ID, -- v.12.2
	SRC_SETTLEMENT_TRADING_BOOK, --v.13.2
	cast('' as varchar(20)) as SRC_BOOK_LEGAL_ENTITY, --v.13.4
	' ' AS TRADE_IND_CATEGORY,
	IS_PROCESSED_TRADE,  ----CSIP-9774
	SRC_COMPETITIVE_FLAG,SRC_PRE_NUM_DEALERS, MESSAGE_ID,SRC_GTM_TRADE_ID,
	cast('' as varchar(1)) as SRC_IS_PROCESSED_TRADE  ----CSIP-9964
into #TDW_LND 
--from dbo.TDW_LND --v.13.4
from #TDW_LND_DATA_PROCESSED --v.13.4
-- v.8.1 where SRC_SYSTEM_CD = @SRC_SYSTEM_CD

if @WithDebug = 1
begin
    select @exectime = 'Step #4.1: @exectime = ' + cast(datediff(ms, @dt, getdate()) as varchar(10)), @dt = getdate()
    print @exectime
end





insert into #TDW_LND
select     RSS_TRADE_SK,
    l.SRC_BOOK_ID,
    l.SRC_CCY_CD, -- v.8.3
    l.SRC_CCY_CD as SRC_SETTLEMENT_CCY,      --v.1.10
    SRC_CUSIP,
    SRC_ISIN,
    l.SRC_E_TRADE_IND,
    EXTRACT_DT,
    SRC_ISSUE_PVBP,
    SRC_MATURITY_DT,
    SRC_SETTLEMENT_DT,
    SRC_ASSIGNED_TO_SALESPERSON_ID,
    SRC_CLIENT_ID,
    SRC_CLIENT_NM,
    SRC_COVERAGE_SALESPERSON_ID,
    SRC_ENTERED_BY_SALESPERSON_ID,
    SRC_LAST_UPDATE_DT,
    --SRC_PRODUCT_CD, v.13.2
    0.0 as  SRC_REVENUE_GRID_AMT, -- 8.3.1 updated later with new formula     
    SRC_REVENUE_SP_MARKUP_AMT,    --v.1.8
    SRC_SECURITY_ID,
    SRC_SECURITY_NM,
    SRC_SUB_ACCOUNT_NM,
    l.SRC_SYSTEM_CD,
    SRC_TRADE_ID,
    SRC_TRADE_STATUS_CD,
    SRC_TRADE_TYPE,
    SRC_TRADE_DT,
    SRC_TRADE_PRICE_AMT,
    SRC_TRADE_PROCEEDS_AMT,
    isnull(SRC_TRADE_QTY, 0) as SRC_TRADE_QTY,
    SRC_TRADE_YIELD,      
    SRC_TRADER_VALUE,   --v.1.8
    --SRC_LEGAL_ENTITY, --v.13.4
	--v.13.4 >>>
	case 
	   when isnull(l.SRC_PROVIDER, '') = 'LAKE' then l.SRC_CPTY_LEGAL_ENTITY
	   else l.SRC_LEGAL_ENTITY
	end as SRC_LEGAL_ENTITY,
	--v.13.4 <<<

    case
        when l.SRC_RISK_CD='NR' 
            and SRC_TRADE_DT >= '2011-11-01' 
            and SRC_COMMENT like 'TWUD%' then 'NR-INVALID'
        else l.SRC_RISK_CD
    end as SRC_RISK_CD,      --v.1.11
    ERROR_CD,
    SRC_TRADE_SUBTYPE,   --v.1.2
    SRC_TRADER_ID, --v.1.6
    SRC_COMMENT,   --v.1.6
    SRC_ORIG_TRADE_SRC, --v.1.6
    SRC_ALLOCATION_COUNT, --v.1.6
    SRC_LONGNOTE_3,      -- v.2.6
	l.SRC_REVENUE_GRID_AMT as REVENUE_GRID_AMT_ORIG,
    SRC_LONGNOTE_2, -- v.3.6
    -- v.4.1
    case -- GRID_INFO
        -- v.13.0 
        when l.SRC_RISK_CD = 'B' then 'RSS-1840: HAIRCUT_MULTIPLIER = 1.0, RISK_CODE_MULTIPLIER_NOT_AXE = 0.0, RISK_CODE_MULTIPLIER_AXE = 1.0'
        -- v.10.1 >>>
        when l.SRC_TRADE_DT >= '2019-10-18' 
            and l.SRC_RISK_CD in('A','AE','SA' /*v.16.0*/)
            and  l.SRC_E_TRADE_IND = 'VOIX' 
            and (
                (l.SRC_BOOK_ID in ('ECREHYLD','ECREHYAE') and l.SRC_CCY_CD in ( 'EUR', 'GBP'))
                or 
                ( hy.RSS_PRODUCT_ID is not null and l.SRC_CCY_CD in ( 'USD')) -- v.12.2
                )              
            then 'CSIP-6112: RISK_CODE_MULTIPLIER_AXE = 1.0, HAIRCUT = 1.0'
        -- v.10.1 <<<
        -- v.9.0 >>>
        when l.SRC_TRADE_DT >= '2019-11-01' 
            and l.SRC_RISK_CD in('A','AE','SA' /*v.16.0*/)
			and l.TRADE_IND_CATEGORY <> 'V'  ----CSIP-9774
            and (
                l.SRC_BOOK_ID in ('ECREHYLD','ECREHYAE') 
                or 
              
                ( c.RSS_PRODUCT_ID is not null and l.SRC_CCY_CD in ( 'EUR', 'GBP')) 
                )  
            and isnull(l.SRC_NUM_DEALERS, 0) not in (1, 2, 3, 4, 5)
            then 'CSIP-5407: RISK_CODE_MULTIPLIER_AXE = 1.0'
        -- v.9.0 <<<
        -- v.10.0 >>>
