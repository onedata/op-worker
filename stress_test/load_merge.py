#!/usr/bin/env python2

## ===================================================================
## @author Krzysztof Trzepla
## @copyright (C): 2014 ACK CYFRONET AGH
## This software is released under the MIT license
## cited in 'LICENSE.txt'.
## ===================================================================
## @doc: This script is used to merge CSV result files describing
## load of each node during stress tests.
## See merge() call below for script arguments.
## ===================================================================

import sys

def merge_lines(lineA, lineB):
    lineA = [ float(x.strip()) for x in lineA.split(",")]
    lineB = [ float(x.strip()) for x in lineB.split(",")]
    line = [str(sum(x)) for x in zip(lineA, lineB)]
    return ",".join(line)

def avg_lines(lines, number):
    result = []
    for line in lines:
        line = [ float(x.strip()) for x in line.split(",")]
        line = map(lambda x: str(x / float(number)), line)
        result.append(",".join(line))
    return result

def merge(input_files, output_file):
    with open(output_file, "w") as output:
        with open(input_files[0], "r") as input:
            header = input.readline()
            result = input.readlines()
        for input_file in input_files[1:]:
            with open(input_file, "r") as input:
                header = input.readline()
                lines = input.readlines()
            result = [merge_lines(lineA, lineB) for (lineA, lineB) in zip(result, lines)]
        result = avg_lines(result, len(input_files))
        output.write(header)
        for line in result:
            output.write(line + "\n")

## All but last argumets are files to be merged into file given as last argument.
## All files has to be with same type (load_log).
merge(sys.argv[1:-1], sys.argv[-1])