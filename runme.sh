#!/bin/bash

# arguments:
# $1 - string, test file name pattern
# $2 - integer, port number
# $3 - string, database name
# $4 - logical, whether to create baseline

n=1
for i in `ls $1`
do
    echo $n.
    Rscript template.R $i $2 $3 $4
    n=$[n+1]
done
