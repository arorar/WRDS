#Global variables
.dbFileName = system.file("extdata", "Cqa.sqlite", package="WRDS")
.tblCrsp ="dow30CRSP" 
.tblCompu="dow30Compustatlink"
.tblSector="sector"

# ################################################################################
# #Fires the given query against database and returns all rows.
# ################################################################################
selectDb <- function(query)
{
  #Connect to database, this looks for a file in your working directory (getwd())
  db.Connection <- dbConnect(SQLite(), dbname=.dbFileName)
  
  #Get records from database
  result.sql <- dbSendQuery(conn = db.Connection,query)
  result.rows <- fetch(result.sql,n=-1)
  
  #Cleanup
  dbClearResult(result.sql)
  
  #Close out any open connection
  dbDisconnect(db.Connection) 
  
  #return records to the caller
  result.rows
}


getReturnTimeSeriesQuery <- function()
{
  return.query <- paste("select distinct c.[date],c.[TICKER],c.[RET] 
                        from",.tblCrsp,
                        "as c where c.[RET] !=0 order by c.[date]")
  strwrap(return.query, width=10000, simplify=TRUE)  
}

getReturnTimeSeries <- function()
{
  return.query <- getReturnTimeSeriesQuery()
  selectDb(return.query)  
}

getReturnPivot <- function()
{
  return.rows <- getReturnTimeSeries()
  cast(return.rows, date ~ TICKER,value='RET')  
}

getFundamentalReturnQuery <- function()
{
  funda.query <- paste("select s.gvkey,s.permno as PERMNO,s.[PERMCO],c.[CUSIP],
                        s.[tic] as TICKER,s.NAME,c.[RET] as [RETURN],s.[GSECTOR],
                        sec.[Sector] ,s.[MarketCap],s.[EntValue],s.[P2B],
                        s.[EV2S],s.[EV2OIBDA],s.datadate as CompuDate,c.[date] 
                        as CrspDate
                        from  ",.tblCrsp,"c inner join ",.tblCompu, 
                        "s on c.[permno]=s.[permno] and c.[permco]=s.[permco]  
                        inner join ",.tblSector,
                        "sec on sec.[GSECTOR] = s.[GSECTOR] 
                        where  c.[date] <= s.[datadate]
                        and substr(c.[date],1,4)=substr(s.[datadate],1,4)  
                        and cast(substr(s.[datadate],6,2) as INT)- 
                        cast(substr(c.[date],6,2) as INT) <=2 ")
  strwrap(funda.query, width=10000, simplify=TRUE)  
}

getFundamentalReturn <- function()
{
  funda.query <- getFundamentalReturnQuery()
  selectDb(funda.query)  
}


