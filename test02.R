library(PivotalR)
db.connect(port = 5333, dbname = "madlib")

## Create a simulated time series and put it into the database
ts <- arima.sim(list(order = c(2,0,1), ar = c(0.7, -0.3), ma=0.2),
                n = 1000000) + 3.2
dat <- data.frame(tid = 1:length(ts), tval = ts)
delete('arima_data1')
as.db.data.frame(dat, "arima_data1",
                 field.types=list(tid="integer", tval="double precision"))

## ----------------------------------------------------------------------

source("perftest.R") # load performance testing functions

perf <- run.test(
    sql = "
        drop table if exists arima_out, arima_out_summary, arima_out_residual;
        select madlib.arima_train(
            'arima_data1',
            'arima_out',
            'tid',
            'tval',
            NULL,
            True,
            array[2,0,1],
            'chunk_size={chunk_size}, max_iter={max_iter}');
        select iter_num from arima_out_summary;",
    params = expand.grid
    (
        chunk_size = c(1000, 10000, 20000, 30000),
        max_iter = c(10, 20)
        ),
    port = 5333, dbname = "madlib", # database information
    time.out = 10 # cancel the query if it takes more than 5 sec
    )

perf
