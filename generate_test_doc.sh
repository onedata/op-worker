#!/bin/bash

echo "Generating documentation for tests in directory doc/test and doc/test_distributed"

escript test/generate_doc.escript
escript test_distributed/generate_doc.escript