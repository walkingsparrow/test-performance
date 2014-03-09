## R framework for measuring MADlib performance

format.str <- function(strs, vals)
{
    replace.str <- names(vals)
    for (i in seq_along(vals))
        strs <- gsub(
            paste("\\{", replace.str[i], "\\}", sep = ""),
            vals[i], strs)
    strs
}

## ----------------------------------------------------------------------

## Older platforms uses procpid, but newer ones use pid
get.pid.col <- function(cid)
{
    cols <- db.q(
        "
        select column_name from information_schema.columns
        where table_name = 'pg_stat_activity'
        ", conn.id = cid, verbose = FALSE, nrows = -1)[ ,1]
    if ("procpid" %in% cols)
        "procpid"
    else
        "pid"
}

## ----------------------------------------------------------------------

## sql -- SQL query to run
## params -- A data.frame that contains all parameters. order does not matter
## fetch.result -- fetch.result result values that you want to record
## cid -- database connection ID
run.test <- function(sql, params, fetch.result, time.out = 3600,
                     host = "localhost", user = Sys.getenv("USER"),
                     dbname = user, password = "", port = 5432,
                     madlib = "madlib", default.schemas = NULL)
{
    suppressMessages(library(parallel))

    mcfork <- parallel:::mcfork
    mcexit <- parallel:::mcexit
    mckill <- parallel:::mckill

    check.interval <- {
        if (time.out > 100)
            60
        else if (time.out > 10)
            10
        else
            1
    }

    if (!is.data.frame(params))
        stop("params must be a data.frame!")

    n <- nrow(params)
    res <- data.frame(
        id = seq_len(n),
        params,
        error = character(n), stringsAsFactors = FALSE)

    time <- numeric(n)
    is.time.out <- logical(n)

    added.fetch.result <- FALSE

    has.fetch.result <- missing(fetch.result) || !(
        length(fetch.result) == 0 || is.null(fetch.result) ||
        is.na(fetch.result) || all(fetch.result == ""))

    suppressMessages(library(PivotalR))
    cid <- db.connect(host=host, user=user, dbname=dbname, port=port,
                      password=password, madlib=madlib,
                      default.schemas=default.schemas, verbose=FALSE)
    pid <- as.integer(db.q("select pg_backend_pid()",
                           conn.id = cid, verbose = FALSE))

    {
        ## two processes
        ## parent to run the SQL
        ## child to cancel a query if too much time has been used
        prcs <- mcfork()

        if (inherits(prcs, "masterProcess")) {
            ## ------------------------------------------------------------
            ## Child process, used to cancel long execution
            tryCatch(
            {
                assign("drv", list(), envir = PivotalR:::.localVars)
                cid1 <- db.connect(
                    host=host, user=user, dbname=dbname, port=port,
                    password=password, madlib=madlib,
                    default.schemas=default.schemas, verbose=FALSE)

                pid.col <- get.pid.col(cid1)

                repeat {
                    Sys.sleep(check.interval)

                    activity <- db.q(
                        "select
                            extract (
                                'epoch' from
                                current_timestamp - query_start) as elapsed
                        from pg_stat_activity
                        where application_name = 'PivotalR' and ",
                        pid.col, " = ", pid, sep = " ",
                        conn.id = cid1, verbose = FALSE, nrows = -1)

                    if (activity$elapsed > time.out)
                        db.q("select pg_cancel_backend(", pid, ")",
                             conn.id = cid1, verbose = FALSE)
                }
            },
                interrupt = function(s) invisible(), # do nothing
                finally = {
                    db.disconnect(conn.id = cid1, verbose = FALSE)
                    mcexit(, "child done")
                })
            ## ------------------------------------------------------------
        } else {
            ## ------------------------------------------------------------
            ## parent process, execute the performance tests
            for (i in seq_len(n)) {
                elapsed <- system.time(run <- try(
                    db.q(format.str(sql, params[i, , drop=FALSE]),
                         conn.id = cid, verbose = FALSE, nrows = -1),
                    silent = TRUE))[3]

                if (is(run, "try-error")) {
                    res$error[i] <- attr(run, "condition")$message
                    if (grepl("canceling statement due to user request",
                              res$error[i]) ||
                        grepl("The backend raised an exception",
                              res$error[i])) {
                        time[i] <- elapsed
                        is.time.out[i] <- TRUE
                    } else {
                        time[i] <- NA
                    }
                } else {
                    ## have not created fetch.result results
                    if (!added.fetch.result) {
                        if (has.fetch.result) {
                            if (missing(fetch.result))
                                fetch.result <- names(run)
                            for (k in seq_along(fetch.result)) {
                                tmp <- rep(run[1,fetch.result[k]], n)
                                tmp[seq_len(i-1)] <- NA
                                res <- cbind(res, tmp)
                                names(res)[ncol(res)] <- fetch.result[k]
                            }
                        }
                        added.fetch.result <- TRUE
                    } else {
                        if (has.fetch.result)
                            res[i,fetch.result] <- run[1,fetch.result]
                    }
                    time[i] <- elapsed
                }
            }

            mckill(prcs)
            ## ------------------------------------------------------------
        }
    }

    db.disconnect(conn.id = cid, verbose = FALSE)
    if(has.fetch.result && added.fetch.result)
        res[is.na(time) | is.time.out, fetch.result] <- NA
    res <- cbind(res, "time (sec)" = time, time.out = is.time.out)
    names(res)[ncol(res)] <- paste("time out (>~", time.out, " sec)", sep = "")
    res
}
