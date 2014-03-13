sql <- "
    drop table if exists arima_out, arima_out_summary, arima_out_residual;
    select madlib.arima_train(
        'ts1e8',
        'arima_out',
        'tid',
        'tval',
        NULL,
        True,
        array[2,0,1],
        'chunk_size={chunk_size}, max_iter={max_iter}');
    select iter_num from arima_out_summary;
    "

params <- expand.grid(
    chunk_size = c(10000L, 20000L, 50000L, 100000L, 200000L, 500000L, 1000000L,
                   2000000L, 5000000L, 10000000L),
    max_iter = 1000L
    )
