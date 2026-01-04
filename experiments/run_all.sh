#!/usr/bin/env bash
set -e

# Usage: ./run_all.sh
# Builds the project and runs the experiment runner, writing CSV to experiments/results.csv

# Build
mvn -q package

# Run the experiment runner (ensure ExperimentRunner exists under package experiments)
# Output is redirected to experiments/results.csv
java -cp target/classes experiments.ExperimentRunner > experiments/results.csv

echo "Results written to experiments/results.csv"