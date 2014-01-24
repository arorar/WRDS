\name{WrdsIndex-helper}

\alias{WrdsIndex-helper}
\alias{getIndexStockQuery}
\alias{getStockFromIndex} 
\alias{getStockXCap}
\alias{getStockLargeCap} 
\alias{getStockMidCap} 
\alias{getStockSmallCap}
\alias{getProminentStocks} 
\title{
    Acceessing merge data from CRSP and COMPUSTAT
}
\description{
    acceessing merge data from CRSP and COMPUSTAT.
}
\usage{
   getIndexStockQuery() 
   getStockFromIndex() 
   getStockXCap(indexName,n)
   getStockLargeCap(n) 
   getStockMidCap(n) 
   getStockSmallCap(n) 
   getProminentStocks(n)
}
\arguments{
  \item{indexName}{
    It takes values in LGCAP, MIDCAP, SMCAP
    }
    \item{n}{
    Number of tickers to select
    }
}
\details{
 \code{getIndexStockQuery}:Returns a query in form of a string
 \code{getStockFromIndex}:Executes the query against the database and returns the rows
 \code{getStockXCap}:Return rows with for Large Cap Index with specific with defaults 10 tickers 
 \code{getStockLargeCap}:
 \code{getStockXCap}:Return rows with for Large Cap Index with specific with specified number of tickers 
 \code{getStockMidCap}:Return rows with for Mid Cap Index with specific with specified number of tickers 
 \code{getStockSmallCap}:Return rows with for Small Cap Index with specific with specified number of tickers 
 \code{getProminentStocks}:Return rows with for Large, Mid and Small Cap Index with specific with specified number of tickers 
}
\author{
   Pankaj Singh
}
\seealso{
 \itemize{
    \item \link{getReturnTimeSeriesQuery} 
    \item \link{getReturnTimeSeries} 
    \item \link{getReturnPivot}
    \item \link{getFundamentalReturnQuery}
    \item \link{getFundamentalReturn}
  }
}
\examples{

require(proto)
require(gsubfn)
require(chron)
require(RSQLite)
require(RSQLite.extfuns)
require(sqldf)

stocks.p <- getProminentStocks(n=20)

}
\keyword{software}
\keyword{data}
