# Restart SQL Server service
# Run this in PowerShell AS ADMINISTRATOR on the cloud server

Write-Host "======================================" -ForegroundColor Cyan
Write-Host "Restarting SQL Server Service" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

try {
    # Stop SQL Server
    Write-Host "Stopping SQL Server (MSSQLSERVER)..." -ForegroundColor Yellow
    Stop-Service MSSQLSERVER -Force
    Start-Sleep -Seconds 3
    Write-Host "[OK] SQL Server stopped" -ForegroundColor Green
    Write-Host ""
    
    # Start SQL Server
    Write-Host "Starting SQL Server (MSSQLSERVER)..." -ForegroundColor Yellow
    Start-Service MSSQLSERVER
    Start-Sleep -Seconds 5
    Write-Host "[OK] SQL Server started" -ForegroundColor Green
    Write-Host ""
    
    # Check status
    $service = Get-Service MSSQLSERVER
    Write-Host "Current Status: $($service.Status)" -ForegroundColor Cyan
    
    if ($service.Status -eq "Running") {
        Write-Host ""
        Write-Host "======================================" -ForegroundColor Green
        Write-Host "[SUCCESS] SQL Server is running!" -ForegroundColor Green
        Write-Host "======================================" -ForegroundColor Green
        Write-Host ""
        Write-Host "You can now test SQL authentication from your local machine:"
        Write-Host "  python test_cloud_quick.py" -ForegroundColor Yellow
    }
    else {
        Write-Host ""
        Write-Host "[WARNING] Service status is: $($service.Status)" -ForegroundColor Red
        Write-Host "Please check SQL Server error logs"
    }
}
catch {
    Write-Host ""
    Write-Host "[ERROR] Failed to restart SQL Server" -ForegroundColor Red
    Write-Host "Error: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "Try manually:"
    Write-Host "  1. Press Win+R → services.msc" -ForegroundColor Yellow
    Write-Host "  2. Find 'SQL Server (MSSQLSERVER)'" -ForegroundColor Yellow
    Write-Host "  3. Right-click → Restart" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Press any key to exit..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

