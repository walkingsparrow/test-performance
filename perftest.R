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

## sql -- SQL query to run
## params -- A data.frame that contains all parameters. order does not matter
## extra -- extra result values that you want to record
## cid -- database connection ID
run.test <- function(sql, params, extra, host = "localhost",
                     user = Sys.getenv("USER"), dbname = user, password = "",
                     port = 5432, madlib = "madlib", default.schemas = NULL)
{
    suppressMessages(library(parallel))

    mcfork <- parallel:::mcfork
    mcexit <- parallel:::mcexit
    sendMaster <- parallel:::sendMaster
    readChild <- parallel:::readChild

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

    ## two processes
    ## parent to run the SQL
    ## child to kill a query if too much time has been used
    {
        prcs <- mcfork()
        if (inherits(prcs, "childProcess")) {
            suppressMessages(library(PivotalR))
            cid <- db.connect(host=host, user=user, dbname=dbname, port=port,
                              password=password, madlib=madlib,
                              default.schemas=default.schemas, verbose=FALSE)

            pid <- unserialize(readChild(prcs))

            for (i in 1:10) {
                Sys.sleep(1)
                activity <- db.q("select * from pg_stat_activity where application_name = 'PivotalR' and pid =",
                                 pid, conn.id = cid, verbose = FALSE, nrows = -1)
                print(activity)
            }

            db.disconnect(conn.id = cid, verbose = FALSE)

            res <- unserialize(readChild(prcs))

            try(detach("package:PivotalR", unload=TRUE), silent = TRUE)

        } else {
            suppressMessages(library(PivotalR))
            cid <- db.connect(host=host, user=user, dbname=dbname, port=port,
                              password=password, madlib=madlib,
                              default.schemas=default.schemas, verbose=FALSE)
            pid <- as.integer(db.q("select pg_backend_pid()", conn.id = cid, verbose = FALSE))
            send <- sendMaster(pid)

            for (i in seq_len(n)) {
                elapsed <- system.time(run <- try(
                    db.q(format.str(sql, params[i, , drop=FALSE]),
                         conn.id = cid, verbose = FALSE, nrows = -1),
                    silent = TRUE))[3]

                if (is(run, "try-error")) {
                    res$error[i] <- attr(run, "condition")$message
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

            db.disconnect(conn.id = cid, verbose = FALSE)
            if(has.extra) res[is.na(time),extra] <- NA
            sendMaster(cbind(res, "time (sec)" = time))
            mcexit(, "master done")
        }
    }

    res
}
