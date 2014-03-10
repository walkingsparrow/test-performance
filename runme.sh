#!/bin/bash

# arguments:
# $1 - string, test file name pattern
# $2 - integer, port number
# $3 - string, database name
# $4 - logical, whether to create baseline

for (i in `ls $1`)
