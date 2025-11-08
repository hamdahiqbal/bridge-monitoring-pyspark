# Run full bridge monitoring pipeline (Windows PowerShell)

Set-Location "$PSScriptRoot\.."

# Activate venv
. .\.venv\Scripts\Activate.ps1

Write-Host "Start a new PowerShell window and run each of these manually:"
Write-Host "1) python .\data_generator\data_generator.py --duration-seconds 60 --rate 10"
Write-Host "2) python .\pipelines\bronze_ingest.py"
Write-Host "3) python .\pipelines\silver_enrichment.py"
Write-Host "4) python .\pipelines\gold_aggregation.py"
