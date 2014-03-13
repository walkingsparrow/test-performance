sql <- "
    drop table if exists arima_out, arima_out_summary, arima_out_residual;
    select madlib.arima_train(
        '{dataset}',
        'arima_out',
        'tid',
        'tval',
        NULL,
        True,
        array[2,0,1],
        'chunk_size=100000, max_iter=1000');
    select iter_num from arima_out_summary;
    "

params <- data.frame(dataset = c("ts1e5", "ts1e6", "ts1e7", "ts1e8", "ts1e9"),
                     stringsAsFactors=FALSE)

