#Global variables
.tblmatchedIndex2 ="matchedIndex2" 

################################################################################
#Returns the query to get stocks that belong to S&P along with days 
#for which it was valid in index
################################################################################
getIndexStockQuery <- function()
{
  stock.query <- paste("select TIC,PERMNO,GVKEY,INDEXTYPE,sum(ValidDays) as 
                       ValidDaysSum 
                       from ",.tblmatchedIndex2,
                       "group by TIC,PERMNO,GVKEY,INDEXTYPE
                       order by IndexType,ValidDaysSum desc" )
  strwrap(stock.query, width=10000, simplify=TRUE)  
}

getStockFromIndex <- function()
{
  stock.query <- getIndexStockQuery()
  selectDb(stock.query)  
}

getStockXCap <- function(indexName='LGCAP',n=10)
{
  stocks<-getStockFromIndex()
  rowIndx<-which(stocks[,"IndexType"]==indexName)
  
  stocks.sub <- stocks[rowIndx,]
  stocks.sub[1:n,]
}

getStockLargeCap <- function(n)
{
  getStockXCap(indexName='LGCAP',n) 
}

getStockMidCap <- function(n)
{
  getStockXCap(indexName='MIDCAP',n) 
}

getStockSmallCap <- function(n)
{
  getStockXCap(indexName='SMCAP',n) 
}

getProminentStocks <- function(n)
{
  lg<-getStockLargeCap(n)
  md<-getStockMidCap(n)
  sm<-getStockSmallCap(n)
  
  rbind(lg,md,sm)
}




