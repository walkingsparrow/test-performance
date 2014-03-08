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
## extra -- extra result values that you want to record
## cid -- database connection ID
run.test <- function(sql, params, extra, time.out = 3600, host = "localhost",
                     user = Sys.getenv("USER"), dbname = user, password = "",
                     port = 5432, madlib = "madlib", default.schemas = NULL)
{
    suppressMessages(library(parallel))

    mcfork <- parallel:::mcfork
    mcexit <- parallel:::mcexit
    mckill <- parallel:::mckill
    check.interval <- if (time.out > 100) 100 else if (time.out > 10) 10 else 1

    if (!is.data.frame(params))
        stop("params must be a data.frame!")

    n <- nrow(params)
    res <- data.frame(
        id = seq_len(n),
        params,
        error = character(n), stringsAsFactors = FALSE)

    time <- numeric(n)

    added.extra <- FALSE

    has.extra <- !(missing(extra) || length(extra) == 0 ||
                   is.null(extra) || is.na(extra) || all(extra == ""))

    suppressMessages(library(PivotalR))
    cid <- db.connect(host=host, user=user, dbname=dbname, port=port,
                      password=password, madlib=madlib,
                      default.schemas=default.schemas, verbose=FALSE)
    pid <- as.integer(db.q("select pg_backend_pid()", conn.id = cid, verbose = FALSE))

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
                              res$error[i]))
                        time[i] <- Inf
                    else
                        time[i] <- NA
                } else {
                    if (!added.extra) { # have not created extra results
                        if (has.extra) {
                            for (k in seq_along(extra)) {
                                tmp <- rep(run[1,extra[k]], n)
                                tmp[seq_len(i-1)] <- NA
                                res <- cbind(res, tmp)
                                names(res)[ncol(res)] <- extra[k]
                            }
                        }
                        added.extra <- TRUE
                    } else {
                        if (has.extra) res[i,extra] <- run[1,extra]
                    }
                    time[i] <- elapsed
                }
            }

            mckill(prcs)
            ## ------------------------------------------------------------
        }
    }

    db.disconnect(conn.id = cid, verbose = FALSE)
    if(has.extra) res[is.na(time) | is.infinite(time), extra] <- NA
    cbind(res, "time (sec)" = time)
}
