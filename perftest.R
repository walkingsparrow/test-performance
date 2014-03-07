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
run.test <- function(sql, params, extra, cid = 1)
{
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

    if(has.extra) res[is.na(time),extra] <- NA

    cbind(res, "time (sec)" = time)
}
