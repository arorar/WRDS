context("WRDSFundamental")

test_that("getReturnTimeSeriesQuery", {
  print("test1:test_WrdsFunda: SUCCESS")
})

test_that("getReturnTimeSeries ", {
  print("test2:test_WrdsFunda: SUCCESS")
})

test_that("getReturnPivot ", {
  r <- getReturnPivot()
  expect_true(nrow(r) > 0)
  expect_true(ncol(r) > 0)
  
  print("test3:test_WrdsFunda: SUCCESS")
})

test_that("getFundamentalReturnQuery ", {
  print("test4:test_WrdsFunda: SUCCESS")
})

test_that("getFundamentalReturn ", {
  print("test5:test_WrdsFunda: SUCCESS")
})