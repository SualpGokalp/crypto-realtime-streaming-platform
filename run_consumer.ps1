# Spark consumer bekcisi: consumer'i baslatir, cokerse 10 sn sonra yeniden baslatir.
# Neden gerekli: bilgisayar uykuya gecince Spark'in heartbeat denetimi executor'u
# olmus sayip tum sureci dusuruyor (bkz. logs/archive/consumer_run_20260831.log).
# Producer'da yeniden baglanma dongusu var, Spark'ta yok — bu script o boslugu kapatir.
# Cikti hem ekrana hem logs\consumer.log'a yazilir (panelin Saglik bolumu okur).
# Durdurmak icin: Ctrl+C ya da pencereyi kapat.
Set-Location $PSScriptRoot
if (-not (Test-Path logs)) { New-Item -ItemType Directory logs | Out-Null }
while ($true) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "[$ts] bekci: consumer baslatiliyor" | Tee-Object -FilePath logs\consumer.log -Append
    .venv\Scripts\python consumer\spark_consumer.py 2>&1 | Tee-Object -FilePath logs\consumer.log -Append
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "[$ts] bekci: consumer durdu (exit $LASTEXITCODE), 10 sn sonra yeniden baslatilacak" |
        Tee-Object -FilePath logs\consumer.log -Append
    Start-Sleep -Seconds 10
}
