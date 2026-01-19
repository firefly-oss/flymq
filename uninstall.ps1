#
# FlyMQ Uninstaller for Windows
# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0
#
# Usage:
#   .\uninstall.ps1                    Interactive uninstall
#   .\uninstall.ps1 -Yes               Non-interactive uninstall
#   .\uninstall.ps1 -RemoveAll         Remove binaries, config, and data
#

param(
    [switch]$Yes = $false,
    [switch]$RemoveData = $false,
    [switch]$RemoveConfig = $false,
    [switch]$RemoveAll = $false,
    [switch]$DryRun = $false,
    [switch]$Backup = $false,
    [string]$BackupDir = "",
    [string]$Prefix = "",
    [switch]$Help = $false
)

$ErrorActionPreference = "Stop"
$SCRIPT_VERSION = "1.26.10"

# Helper Functions
function Write-Success { param([string]$msg) Write-Host "✓ $msg" -ForegroundColor Green }
function Write-ErrorMsg { param([string]$msg) Write-Host "✗ $msg" -ForegroundColor Red }
function Write-WarningMsg { param([string]$msg) Write-Host "⚠ $msg" -ForegroundColor Yellow }
function Write-Info { param([string]$msg) Write-Host "ℹ $msg" -ForegroundColor Cyan }
function Write-Step { param([string]$msg) Write-Host "`n==> $msg" -ForegroundColor Cyan }

function Show-Banner {
    Write-Host ""
    Write-Host "  FlyMQ Windows Uninstaller" -ForegroundColor Yellow
    Write-Host "  Version $SCRIPT_VERSION" -ForegroundColor DarkGray
    Write-Host ""
}

function Get-DefaultPrefix { return "$env:LOCALAPPDATA\FlyMQ" }
function Get-DefaultDataDir { return "$env:LOCALAPPDATA\FlyMQ\data" }
function Get-DefaultConfigDir { return "$env:LOCALAPPDATA\FlyMQ\config" }

function Find-Installation {
    $prefix = if ($Prefix) { $Prefix } else { Get-DefaultPrefix }
    $binDir = Join-Path $prefix "bin"
    $result = @{
        Found = $false; Prefix = $prefix; BinDir = $binDir
        ConfigDir = Get-DefaultConfigDir; DataDir = Get-DefaultDataDir
    }
    if (Test-Path (Join-Path $binDir "flymq.exe")) { $result.Found = $true }
    return $result
}

function Stop-FlyMQProcesses {
    Write-Step "Stopping FlyMQ processes"
    $processes = Get-Process -Name "flymq*" -ErrorAction SilentlyContinue
    if ($processes) {
        if ($DryRun) { Write-Info "[DRY RUN] Would stop $($processes.Count) process(es)" }
        else { $processes | Stop-Process -Force; Write-Success "Stopped $($processes.Count) process(es)" }
    } else { Write-Info "No FlyMQ processes running" }
}

function Backup-BeforeRemoval {
    param([hashtable]$inst)
    if (-not $Backup -and -not $BackupDir) { return }
    Write-Step "Creating backup"
    $backupBase = if ($BackupDir) { $BackupDir } else { "$env:USERPROFILE\flymq-backup" }
    $backupPath = Join-Path $backupBase "flymq-backup-$(Get-Date -Format 'yyyyMMdd_HHmmss')"
    New-Item -ItemType Directory -Path $backupPath -Force | Out-Null
    if (Test-Path $inst.ConfigDir) {
        if ($DryRun) { Write-Info "[DRY RUN] Would backup config" }
        else { Copy-Item $inst.ConfigDir (Join-Path $backupPath "config") -Recurse; Write-Success "Backed up config" }
    }
    if (($RemoveData -or $RemoveAll) -and (Test-Path $inst.DataDir)) {
        if ($DryRun) { Write-Info "[DRY RUN] Would backup data" }
        else { Copy-Item $inst.DataDir (Join-Path $backupPath "data") -Recurse; Write-Success "Backed up data" }
    }
    Write-Success "Backup: $backupPath"
}

function Remove-Binaries {
    param([hashtable]$inst)
    Write-Step "Removing binaries"
    foreach ($bin in @("flymq.exe", "flymq-cli.exe", "flymq-discover.exe")) {
        $binPath = Join-Path $inst.BinDir $bin
        if (Test-Path $binPath) {
            if ($DryRun) { Write-Info "[DRY RUN] Would remove: $bin" }
            else { Remove-Item $binPath -Force; Write-Success "Removed $bin" }
        }
    }
}

function Remove-DataDirectory {
    param([hashtable]$inst)
    if (-not $RemoveData -and -not $RemoveAll) { return }
    Write-Step "Removing data directory"
    if (Test-Path $inst.DataDir) {
        if ($DryRun) { Write-Info "[DRY RUN] Would remove: $($inst.DataDir)" }
        else { Remove-Item $inst.DataDir -Recurse -Force; Write-Success "Removed data" }
    }
}

function Remove-ConfigDirectory {
    param([hashtable]$inst)
    if (-not $RemoveConfig -and -not $RemoveAll) { return }
    Write-Step "Removing config directory"
    if (Test-Path $inst.ConfigDir) {
        if ($DryRun) { Write-Info "[DRY RUN] Would remove: $($inst.ConfigDir)" }
        else { Remove-Item $inst.ConfigDir -Recurse -Force; Write-Success "Removed config" }
    }
}

function Show-Completion {
    param([hashtable]$inst)
    Write-Host "`n"
    if ($DryRun) { Write-Host "  DRY RUN COMPLETE" -ForegroundColor Cyan; Write-Info "No changes made" }
    else { Write-Host "  ✓ UNINSTALL COMPLETE" -ForegroundColor Green; Write-Success "FlyMQ removed" }
    if (-not $RemoveData -and (Test-Path $inst.DataDir)) { Write-Info "Data preserved: $($inst.DataDir)" }
    if (-not $RemoveConfig -and (Test-Path $inst.ConfigDir)) { Write-Info "Config preserved: $($inst.ConfigDir)" }
    Write-Host "`n  Thank you for using FlyMQ!`n" -ForegroundColor DarkGray
}

# Main
Show-Banner
if ($Help) {
    Write-Host "Usage: .\uninstall.ps1 [OPTIONS]`n"
    Write-Host "Options:"
    Write-Host "  -Yes            Skip prompts    -RemoveData     Remove data"
    Write-Host "  -RemoveConfig   Remove config   -RemoveAll      Remove all"
    Write-Host "  -Backup         Backup first    -BackupDir DIR  Backup location"
    Write-Host "  -DryRun         Preview only    -Prefix PATH    Install prefix"
    Write-Host "  -Help           Show help`n"
    exit 0
}
if ($RemoveAll) { $RemoveData = $true; $RemoveConfig = $true }
if ($BackupDir) { $Backup = $true }

Write-Step "Detecting installation"
$inst = Find-Installation
if (-not $inst.Found) { Write-WarningMsg "No FlyMQ installation found"; exit 1 }
Write-Success "Found: $($inst.BinDir)"

if (-not $Yes) {
    $confirm = Read-Host "`n  Proceed with uninstallation? (y/N)"
    if ($confirm -ne "y" -and $confirm -ne "Y") { Write-Info "Cancelled"; exit 0 }
}

Stop-FlyMQProcesses
Backup-BeforeRemoval -inst $inst
Remove-Binaries -inst $inst
Remove-DataDirectory -inst $inst
Remove-ConfigDirectory -inst $inst
Show-Completion -inst $inst

