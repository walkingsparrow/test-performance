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

colourise <- function (text, fg = "black", bg = NULL)
{
    term <- Sys.getenv()["TERM"]
    colour_terms <- c("xterm-color", "xterm-256color", "screen",
        "screen-256color")
    if (!any(term %in% colour_terms, na.rm = TRUE)) {
        return(text)
    }
    col_escape <- function(col) {
        paste0("\033[", col, "m")
    }
    col <- .fg_colours[tolower(fg)]
    if (!is.null(bg)) {
        col <- paste0(col, .bg_colours[tolower(bg)], sep = ";")
    }
    init <- col_escape(col)
    reset <- col_escape("0")
    paste0(init, text, reset)
}

.fg_colours <- c(
  "black" = "0;30",
  "blue" = "0;34",
  "green" = "0;32",
  "cyan" = "0;36",
  "red" = "0;31",
  "purple" = "0;35",
  "brown" = "0;33",
  "light gray" = "0;37",
  "dark gray" = "1;30",
  "light blue" = "1;34",
  "light green" = "1;32",
  "light cyan" = "1;36",
  "light red" = "1;31",
  "light purple" = "1;35",
  "yellow" = "1;33",
  "white" = "1;37"
)

.bg_colours <- c(
  "black" = "40",
  "red" = "41",
  "green" = "42",
  "brown" = "43",
  "blue" = "44",
  "purple" = "45",
  "cyan" = "46",
  "light gray" = "47"
)

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
            10
        else if (time.out > 10)
            5
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
                    stop("Performance tests are interrupted!")
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
                    cat(paste(names(params), "=", params[i,],
                              collapse = ", ", sep = ""), "...... ")

                    elapsed <- system.time(run <- try(
                        db.q(format.str(sql, params[i, , drop=FALSE]),
                             conn.id = cid, verbose = FALSE, nrows = -1),
                        silent = TRUE))[3]

                    ## to avoid cancelling the next query
                    Sys.sleep(1)

                    if (is(run, "try-error")) {
                        res$test_error[i] <- gsub(
                            "(\n|,)", " ", attr(run, "condition")$message)
                        if (grepl("canceling statement due to user request",
                                  res$test_error[i]) ||
                            grepl("The backend raised an exception",
                                  res$test_error[i])) {
                            time[i] <- elapsed
                            is.time.out[i] <- TRUE
                        } else {
                            time[i] <- NA
                        }

                        if (is.na(time[i])) err.str <- "Error"
                        else err.str <- "Timeout"

                        if (!missing(history)) {
                            history.time <- history[i, ncol(history)-1]
                            history.timeout <- history[i, ncol(history)]
                            if (is.na(history.time)) {
                                cat(colourise(paste(err.str, " (and baseline raises an error)\n", sep = ""), "red"))
                            } else if (history.timeout) {
                                cat(colourise(paste(err.str, " (and baseline fails due to timeout)\n", sep = ""), "red"))
                            } else {
                                cat(colourise(paste(err.str, " (but baseline is fine)\n", sep = ""), "red"))
                            }
                        } else {
                            cat(colourise(paste(err.str, "\n", sep = ""), "red"))
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
                            if (time[i] < history.time*(1-compare.threshold)) {
                                cat(colourise(paste(
                                    format((1-time[i]/history.time)*100,
                                           digits=2, width=4),
                                    "% better\n", sep = ""), "green"))
                            } else if (time[i] > history.time*
                                       (1 + compare.threshold)) {
                                cat(colourise(paste(
                                    format((time[i]/history.time-1)*100,
                                           digits=2, width=4),
                                    "% worse\n", sep = ""), "red"))
                            } else {
                                cat(colourise(paste(
                                    "similar within ",
                                    format(compare.threshold*100, digits=2),
                                    "%\n", sep = ""), "yellow"))
                            }
                        }  else {
                            cat("Done\n")
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
