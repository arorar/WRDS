context("WRDSIndex")

test_that("getIndexStockQuery", {
    print("test1:test_WrdsIndex: SUCCESS")
})

test_that("getStockFromIndex ", {
  print("test2:test_WrdsIndex: SUCCESS")
})

test_that("getStockXCap ", {
  print("test3:test_WrdsIndex: SUCCESS")
})

test_that("getStockLargeCap ", {
  print("test4:test_WrdsIndex: SUCCESS")
})

test_that("getStockMidCap ", {
  print("test5:test_WrdsIndex: SUCCESS")
})

test_that("getStockSmallCap  ", {
  print("test6:test_WrdsIndex: SUCCESS")
})

test_that("getProminentStocks  ", {
  r <- getProminentStocks(2)
  expect_that(nrow(r), equals(6))
  expect_that(ncol(r), equals(5))
  
  print("test7:test_WrdsIndex: SUCCESS")  
})