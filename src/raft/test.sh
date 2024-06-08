#!/bin/bash

count=0

max_tests=1000

# test_case="2A"
# test_case="2B"
# test_case="2C"
test_case="TestFigure8Unreliable2C"

for ((i=1; i<=max_tests; i++))
do
    echo "====== iteration $i ======"

    go test -v -run ${test_case}

    if [ "$?" -eq 0 ]; then
        echo "====== iteration $i passed. ======"
    else
        break
    fi
done