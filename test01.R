library(PivotalR)

db.connect(port = 5333, dbname = "madlib")

source("perftest.R")

ts <- arima.sim(list(order = c(2,0,1), ar = c(0.7, -0.3), ma=0.2), n = 1000000) + 3.2

dat <- data.frame(tid = 1:length(ts), tval = ts)

delete('arima_data')
as.db.data.frame(dat, "arima_data", field.types=list(tid="integer", tval="double precision"))

perf <- run.test(
    sql = "
        drop table if exists arima_out, arima_out_summary, arima_out_residual;
        select madlib.arima_train(
            'arima_data',
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
        max_iter = c(10, 20)),
    extra = "iter_num")

perf

## ----------------------------------------------------------------------

library(fork)

{
    pid = fork(slave=NULL)
    if(pid==0) {
        cat("Hi from the child process\n")
        db.list()
        exit()
    } else {
        cat("Hi from the parent process\n")
        db.list()
    }
}
