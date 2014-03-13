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
    chunk_size = 100000L,
    max_iter = 1000L
    )
