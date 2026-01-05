# PowerShell script to run experiments and save complete results to CSV
# Usage: .\experiments\run_tests.ps1

param()
$ErrorActionPreference = 'Stop'

# Compile the project
Write-Host "Compiling project..."
mvn compile

# Create output file with header
Write-Host "Creating output CSV file..."
$outputFile = "experiments\results.csv"
"experiment,metric,value" | Set-Content -Path $outputFile -Encoding UTF8

# Run the experiment and append results to CSV
Write-Host "Running experiments..."
java -cp target/classes experiments.ExperimentRunner | Add-Content -Path $outputFile -Encoding UTF8

Write-Host "Done! Results saved to $outputFile"

# Show the first few lines of the results
Write-Host "\nFirst few lines of results:"
Get-Content -Path $outputFile -Head 20