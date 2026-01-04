# One-click runner for Windows PowerShell
# Usage: Open PowerShell in project root and run: .\experiments\run_all.ps1

param()
$ErrorActionPreference = 'Stop'

Write-Host "Building project with Maven..."
mvn -DskipTests package

Write-Host "Running experiments (requires experiments.ExperimentRunner main)..."
java -cp target/classes experiments.ExperimentRunner > experiments\results.csv

Write-Host "Done. Results written to experiments\results.csv"