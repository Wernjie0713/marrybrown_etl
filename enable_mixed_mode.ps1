# Enable SQL Server Mixed Mode Authentication
# Run this in PowerShell AS ADMINISTRATOR on the cloud server

Write-Host "======================================" -ForegroundColor Cyan
Write-Host "SQL Server Mixed Mode Enabler" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

# Check if running as Administrator
$currentPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
$isAdmin = $currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Write-Host "[ERROR] This script must be run as Administrator!" -ForegroundColor Red
    Write-Host "Please right-click PowerShell and select 'Run as Administrator'" -ForegroundColor Yellow
    Write-Host ""
    Pause
    Exit
}

Write-Host "[OK] Running as Administrator" -ForegroundColor Green
Write-Host ""

# Registry path for SQL Server authentication mode
$regPath = "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQLServer"

# Check if path exists
if (-not (Test-Path $regPath)) {
    Write-Host "[ERROR] SQL Server registry path not found!" -ForegroundColor Red
    Write-Host "Path: $regPath" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Possible issues:" -ForegroundColor Yellow
    Write-Host "1. SQL Server not installed" -ForegroundColor Yellow
    Write-Host "2. Different instance name (not MSSQLSERVER)" -ForegroundColor Yellow
    Write-Host "3. Different SQL Server version" -ForegroundColor Yellow
    Write-Host ""
    Pause
    Exit
}

try {
    # Get current LoginMode
    Write-Host "Step 1: Checking current authentication mode..." -ForegroundColor Yellow
    $currentMode = Get-ItemProperty -Path $regPath -Name "LoginMode" -ErrorAction Stop
    
    if ($currentMode.LoginMode -eq 1) {
        Write-Host "[INFO] Current Mode: Windows Authentication Only" -ForegroundColor Yellow
    }
    elseif ($currentMode.LoginMode -eq 2) {
        Write-Host "[INFO] Current Mode: Mixed Mode (Already enabled!)" -ForegroundColor Green
        Write-Host ""
        Write-Host "Mixed mode is already enabled. If SQL logins still don't work:" -ForegroundColor Cyan
        Write-Host "1. Make sure SQL Server has been restarted AFTER enabling mixed mode" -ForegroundColor Cyan
        Write-Host "2. Verify sa account is enabled (run reset_sa_password.sql)" -ForegroundColor Cyan
        Write-Host "3. Check Windows Firewall (port 1433)" -ForegroundColor Cyan
        Write-Host ""
    }
    else {
        Write-Host "[WARNING] Unknown LoginMode value: $($currentMode.LoginMode)" -ForegroundColor Yellow
    }
    
    Write-Host ""
    Write-Host "Step 2: Enabling Mixed Mode Authentication..." -ForegroundColor Yellow
    
    # Set LoginMode to 2 (Mixed Mode)
    Set-ItemProperty -Path $regPath -Name "LoginMode" -Value 2 -Type DWord
    
    Write-Host "[OK] Registry value updated!" -ForegroundColor Green
    Write-Host ""
    
    # Verify the change
    Write-Host "Step 3: Verifying change..." -ForegroundColor Yellow
    $newMode = Get-ItemProperty -Path $regPath -Name "LoginMode"
    
    if ($newMode.LoginMode -eq 2) {
        Write-Host "[OK] LoginMode = 2 (Mixed Mode enabled in registry)" -ForegroundColor Green
    }
    else {
        Write-Host "[ERROR] LoginMode is still: $($newMode.LoginMode)" -ForegroundColor Red
        Write-Host "Please try running this script again or use SQL Server Configuration Manager" -ForegroundColor Yellow
    }
    
    Write-Host ""
    Write-Host "======================================" -ForegroundColor Cyan
    Write-Host "RESTART SQL SERVER (REQUIRED)" -ForegroundColor Cyan
    Write-Host "======================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "The change will NOT take effect until SQL Server is restarted!" -ForegroundColor Yellow
    Write-Host ""
    
    $restart = Read-Host "Do you want to restart SQL Server now? (Y/N)"
    
    if ($restart -eq "Y" -or $restart -eq "y") {
        Write-Host ""
        Write-Host "Restarting SQL Server..." -ForegroundColor Yellow
        
        # Stop SQL Server
        Write-Host "Stopping SQL Server (MSSQLSERVER)..." -ForegroundColor Yellow
        Stop-Service MSSQLSERVER -Force
        Start-Sleep -Seconds 3
        Write-Host "[OK] SQL Server stopped" -ForegroundColor Green
        
        # Start SQL Server
        Write-Host "Starting SQL Server (MSSQLSERVER)..." -ForegroundColor Yellow
        Start-Service MSSQLSERVER
        Start-Sleep -Seconds 5
        
        # Check status
        $service = Get-Service MSSQLSERVER
        if ($service.Status -eq "Running") {
            Write-Host "[OK] SQL Server is running!" -ForegroundColor Green
            Write-Host ""
            Write-Host "======================================" -ForegroundColor Green
            Write-Host "[SUCCESS] Mixed Mode Enabled!" -ForegroundColor Green
            Write-Host "======================================" -ForegroundColor Green
            Write-Host ""
            Write-Host "You can now test SQL authentication:" -ForegroundColor Cyan
            Write-Host "  - From local machine: python test_cloud_quick.py" -ForegroundColor White
            Write-Host "  - Username: sa or etl_user" -ForegroundColor White
            Write-Host "  - Password: (the one you set)" -ForegroundColor White
        }
        else {
            Write-Host "[WARNING] Service status: $($service.Status)" -ForegroundColor Red
            Write-Host "Please check SQL Server error logs" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host ""
        Write-Host "[INFO] Restart skipped. Remember to restart SQL Server manually!" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "To restart manually:" -ForegroundColor Cyan
        Write-Host "  Restart-Service MSSQLSERVER" -ForegroundColor White
        Write-Host ""
    }
}
catch {
    Write-Host ""
    Write-Host "[ERROR] Failed to enable mixed mode:" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Write-Host ""
    Write-Host "Alternative methods:" -ForegroundColor Yellow
    Write-Host "1. Use SQL Server Configuration Manager" -ForegroundColor White
    Write-Host "2. Run enable_mixed_mode_v2.sql in SSMS (as Administrator)" -ForegroundColor White
}

Write-Host ""
Write-Host "Press any key to exit..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

