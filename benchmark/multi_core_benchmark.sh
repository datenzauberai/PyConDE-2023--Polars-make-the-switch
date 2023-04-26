#!/bin/bash
# test all cores: ./multi_core_benchmark.sh | tee multi_core_benchmark_result.csv
# test up to 16 cores: ./multi_core_benchmark.sh 16 | tee multi_core_benchmark_result.csv

# Maximum number of cores to be benchmarked as argument or from nproc
if [ -z "$1" ]
then
  num_cores=$(nproc --all)
else
  num_cores="$1"
fi

echo library,mode,workers,execution time
for (( n=1; n<=$num_cores; n++ ))
do
   python simple_benchmark.py polars --w $n --r 10 --lazy
   python simple_benchmark.py polars --w $n --r 5
   python simple_benchmark.py duckdb --w $n --r 5
done
