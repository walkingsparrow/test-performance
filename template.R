## template to run the performance tests

source("perftest.R") # load performance testing functions

## re-order the parameters, so that
## change of order does not raise an error when comparing
## with baseline file
params <- eval(parse(
    text = paste("params[with(params, order(",
    paste(names(params), collapse = ","), ")),]")))

args <- commandArgs(TRUE)
port <- as.integer(args[1])
dbname <- args[2]

create.baseline <- args[3]
script.name <- sub(".*=", "", commandArgs()[4])

message("Start the performance testing of ", script.name, " ...")

baseline <- paste("baseline_", sub("\\.r$", "", script.name), ".csv",
                  sep = "")
result <- paste("result_", sub("\\.r$", "", script.name), ".csv",
                  sep = "")
if (!create.baseline) { # use baseline file
    history <- read.csv(baseline)
    col1 <- which(names(history) == "test_id") + 1
    col2 <- which(names(history) == "test_error") - 1
    origin.params <- history[, col1:col2]
    ## params is always ordered, so we can do the following comparison
    if (all.equal(origin.params, params) != TRUE)
        stop("The baseline file has a parameter set ",
             "that is different from the set defined in ",
             script.name, ". You may need to recreate the baseline file.")

    perf <- run.test(
        sql = sql,
        params = params, history = history,
        port = port, dbname = dbname, # database information
        time.out = 3600 # cancel the query if it takes more than 5 sec
        )

    write.csv(perf, result, quote = FALSE, row.names = FALSE)
} else {

    perf <- run.test(
        sql = sql,
        params = params,
        port = port, dbname = dbname, # database information
        time.out = 3600 # cancel the query if it takes more than 5 sec
        )

    write.csv(perf, baseline, quote = FALSE, row.names = FALSE)
}

message("The performance testing of ", script.name, " is done.\n")
