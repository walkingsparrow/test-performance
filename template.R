## template to run the performance tests

source("perftest.R") # load performance testing functions

args <- commandArgs(TRUE)
port <- as.integer(args[2])
dbname <- args[3]

create.baseline <- as.logical(args[4])
if (is.na(create.baseline))
    stop("Use 'true' or 'false' to indicate whether to ",
         "create (or compare with) the baseline file !")

script.name <- args[1]

source(script.name)

## re-order the parameters, so that
## change of order does not raise an error when comparing
## with baseline file
params <- data.frame(eval(parse(
    text = paste("params[with(params, order(",
    paste(names(params), collapse = ","), ")),,drop=FALSE]"))))
row.names(params) <- 1:nrow(params)

baseline <- paste(sub("(\\.r$|\\.R$)", "", script.name), "_baseline.csv",
                  sep = "")
result <- paste(sub("(\\.r$|\\.R$)", "", script.name), "_result.csv",
                sep = "")

if (!create.baseline) {
    compare.str <- paste(", compared with baseline ", baseline, sep = "")
} else {
    compare.str <- ""
}

message("Start the performance testing of ", script.name, compare.str, " ...")

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

if (create.baseline) {
    base.str <- paste("\nBaseline file ", result, " has been created.", sep = "")
} else {
    base.str <- ""
}

message("The performance testing of ", script.name, " is done.", base.str, "\n")
