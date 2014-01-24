\name{WrdsFunda-helpers}

\alias{WrdsFunda-helpers}
\alias{getReturnTimeSeriesQuery}
\alias{getReturnTimeSeries}
\alias{getReturnPivot}
\alias{getFundamentalReturnQuery}
\alias{getFundamentalReturn}

\title{
    Accessing merge data from CRSP and COMPUSTAT
}
\description{
    Acceessing merge data from CRSP and COMPUSTAT
}
\usage{
    getReturnTimeSeriesQuery()
    getReturnTimeSeries()
    getReturnPivot()
    getFundamentalReturnQuery()
    getFundamentalReturn()
}
\details{
    \code{getReturnTimeSeriesQuery}:
    Returns a query in form of a string
    
    \code{getReturnTimeSeries}:
    Executes the query against the database and returns the rows
    
    \code{getReturnPivot}:
    Converts the data into timeseries of ticker values

    \code{getFundamentalReturnQuery}:
    Returns a query in form of a string
    
    \code{getFundamentalReturn}:
    Executes the query against the database and returns the rows    
}
\author{
    Pankaj Singh
}
\seealso{
\itemize{
    \item \link{getIndexStockQuery} 
    \item \link{getStockFromIndex}
    \item \link{getStockXCap} 
    \item \link{getStockLargeCap}
    \item \link{getStockMidCap} 
    \item \link{getStockSmallCap}
    \item \link{getProminentStocks} 
  }
}
\examples{

#Get combined data(fundamental from Compustat+returns from CRSP)
funda.return <- getFundamentalReturn() 
head(funda.return)

#GetTimeSeries data(returns for all stocks)
pivot.return<-getReturnPivot()
head(pivot.return)

}
\keyword{software}
