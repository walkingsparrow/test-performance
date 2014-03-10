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
run.test <- function(sql, params, history, fetch.result,
                     compare.threshold = 0.1, time.out = 3600,
                     host = "localhost", user = Sys.getenv("USER"),
                     dbname = user, password = "", port = 5432,
                     madlib = "madlib", default.schemas = NULL)
{
    suppressMessages(library(parallel))

    mcfork <- parallel:::mcfork
    mcexit <- parallel:::mcexit
    mckill <- parallel:::mckill
    selectChildren <- parallel:::selectChildren
    sendMaster <- parallel:::sendMaster
    readChild <- parallel:::readChild
    sendChildStdin <- parallel:::sendChildStdin

    check.interval <- {
        if (time.out > 60)
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
        test_id = seq_len(n),
        params,
        test_error = character(n), stringsAsFactors = FALSE)

    time <- numeric(n)
    is.time.out <- logical(n)

    added.fetch.result <- FALSE

    has.fetch.result <- missing(fetch.result) || !(
        length(fetch.result) == 0 || is.null(fetch.result) ||
        is.na(fetch.result) || all(fetch.result == ""))

    suppressMessages(library(PivotalR))

    cid1 <- db.connect(
        host=host, user=user, dbname=dbname, port=port,
        password=password, madlib=madlib,
        default.schemas=default.schemas, verbose=FALSE, quick = TRUE)

    {
        ## two processes
        ## parent to run the SQL
        ## child to cancel a query if too much time has been used
        prcs <- mcfork()

        if (inherits(prcs, "childProcess")) {
            ## ------------------------------------------------------------
            ## Master process, used to cancel long execution
            tryCatch(
            {
                pid.col <- get.pid.col(cid1)
                pid <- unserialize(readChild(prcs))

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

                    if (!is.null(activity$elapsed) &&
                        activity$elapsed > time.out)
                        db.q("select pg_cancel_backend(", pid, ")",
                             conn.id = cid1, verbose = FALSE)

                    available <- selectChildren(prcs)
                    if (!is.logical(available) && available == prcs$pid) {
                        res <- unserialize(readChild(prcs))
                        capture.output(sendChildStdin(prcs, "OK\n"),
                                       file = "/dev/null")
                        break
                    }
                }
            },
                interrupt = function(s) {
                    db.q("select pg_cancel_backend(", pid, ")",
                         conn.id = cid1, verbose = FALSE)
                    mckill(prcs, signal = 9)
                },
                finally = {
                    mckill(prcs, signal = 9)
                    db.disconnect(conn.id = cid1, verbose = FALSE)
                })
            ## ------------------------------------------------------------
        } else {
            ## ------------------------------------------------------------
            ## Child process, execute the performance tests
            assign("drv", list(), envir = PivotalR:::.localVars)
            cid <- db.connect(host=host, user=user, dbname=dbname, port=port,
                              password=password, madlib=madlib,
                              default.schemas=default.schemas, verbose=FALSE,
                               quick = TRUE)

            pid <- as.integer(db.q("select pg_backend_pid()",
                                   conn.id = cid, verbose = FALSE))

            sendMaster(pid)
            tryCatch({
                for (i in seq_len(n)) {
                    elapsed <- system.time(run <- try(
                        db.q(format.str(sql, params[i, , drop=FALSE]),
                             conn.id = cid, verbose = FALSE, nrows = -1),
                        silent = TRUE))[3]

                    ## to avoid cancelling the next query
                    Sys.sleep(1)

                    if (is(run, "try-error")) {
                        res$test_error[i] <- attr(run, "condition")$message
                        if (grepl("canceling statement due to user request",
                                  res$test_error[i]) ||
                            grepl("The backend raised an exception",
                                  res$test_error[i])) {
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

                        if (!missing(history)) {
                            history.time <- history[i, ncol(history)-1]
                            cat(paste(names(params), "=", params[i,],
                                      collapse = ", "), "...... ")
                            if (time[i] < history.time*(1-compare.threshold)) {
                                cat(format((1-history.time/time[i])*100,
                                           digits=2),
                                    "% better\n", sep = "")
                            } else if (time[i] > history.time*
                                       (1 - compare.threshold)) {
                                cat(format((history.time/time[i]-1)*100,
                                           digits=2),
                                    "% worse\n", sep = "")
                            } else {
                                cat("similar within ",
                                    format(compare.threshold*100, digits=2),
                                    "%\n", sep = "")
                            }
                        }
                    }
                }

                if(has.fetch.result && added.fetch.result)
                    res[is.na(time) | is.time.out, fetch.result] <- NA
                res <- cbind(res, "time (sec)" = time, time.out = is.time.out)
                names(res)[ncol(res)] <- paste("timeout (>~", time.out,
                                               " sec)", sep = "")
                sendMaster(res)
                capture.output(readline(), file = "/dev/null")
            },
                     interrupt = function(s) invisible(),
                     finally = {
                         db.disconnect(conn.id = cid, verbose = FALSE)
                         mcexit(, "child done")
                     })
            ## ------------------------------------------------------------
        }
    }

    res
}
