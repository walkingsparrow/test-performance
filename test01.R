a <- expand.grid(chunk_size = c(1e4, 1e5, 1e5, 1e7), size = c(1e8))

format.str <- function(strs, vals)
{
    replace.str <- names(vals)
    for (i in seq_along(vals))
        strs <- gsub(
            paste("\\{", replace.str[i], "\\}", sep = ""),
            vals[i], strs)
    strs
}


format.str("{chunk_size} is {size}", a[1,])
