# =============================================================================
# Project Babe Avalanche — Windows Bootstrap Script
# =============================================================================
# Run this in an ELEVATED PowerShell (Run as Administrator)
#
# Usage:
#   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
#   .\scripts\bootstrap_windows.ps1
# =============================================================================

param(
    [string]$OllamaModel = "llama3.2:3b",
    [switch]$SkipOllama,
    [switch]$SkipClaudeCode
)

$ErrorActionPreference = "Stop"

function Write-Step($msg) {
    Write-Host "`n==== $msg ====" -ForegroundColor Cyan
}

function Test-Command($cmd) {
    return [bool](Get-Command $cmd -ErrorAction SilentlyContinue)
}

# ---- Check for admin (needed for winget installs) ----
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "WARNING: Not running as Administrator. Some installs may fail." -ForegroundColor Yellow
    Write-Host "Recommendation: Right-click PowerShell -> Run as Administrator" -ForegroundColor Yellow
    $continue = Read-Host "Continue anyway? (y/n)"
    if ($continue -ne "y") { exit 1 }
}

# ---- Python ----
Write-Step "Checking Python"
if (Test-Command "python") {
    $pyVer = python --version 2>&1
    Write-Host "Found: $pyVer" -ForegroundColor Green
} else {
    Write-Host "Installing Python via winget..." -ForegroundColor Yellow
    winget install Python.Python.3.12 --accept-package-agreements --accept-source-agreements
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
    if (Test-Command "python") {
        Write-Host "Python installed: $(python --version)" -ForegroundColor Green
    } else {
        Write-Host "Python installed but not in PATH yet. You may need to restart this terminal." -ForegroundColor Yellow
    }
}

# ---- Node.js ----
Write-Step "Checking Node.js"
if (Test-Command "node") {
    $nodeVer = node --version 2>&1
    Write-Host "Found: Node.js $nodeVer" -ForegroundColor Green
} else {
    Write-Host "Installing Node.js LTS via winget..." -ForegroundColor Yellow
    winget install OpenJS.NodeJS.LTS --accept-package-agreements --accept-source-agreements
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
    if (Test-Command "node") {
        Write-Host "Node.js installed: $(node --version)" -ForegroundColor Green
    } else {
        Write-Host "Node.js installed but not in PATH yet. You may need to restart this terminal." -ForegroundColor Yellow
    }
}

# ---- Git (should already exist if you cloned this repo) ----
Write-Step "Checking Git"
if (Test-Command "git") {
    Write-Host "Found: $(git --version)" -ForegroundColor Green
} else {
    Write-Host "Installing Git via winget..." -ForegroundColor Yellow
    winget install Git.Git --accept-package-agreements --accept-source-agreements
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
}

# ---- Python venv + dependencies ----
Write-Step "Setting up Python virtual environment"
$venvPath = Join-Path $PSScriptRoot ".." ".venv"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

Push-Location $projectRoot

if (-not (Test-Path ".venv")) {
    Write-Host "Creating virtual environment..." -ForegroundColor Yellow
    python -m venv .venv
}

Write-Host "Activating venv..." -ForegroundColor Yellow
& .venv\Scripts\Activate.ps1

Write-Host "Installing core dependencies..." -ForegroundColor Yellow
pip install --upgrade pip
pip install -r requirements.txt

Write-Host "Installing insights generator dependencies..." -ForegroundColor Yellow
pip install -r insights_generator/requirements.txt

Write-Host "Python environment ready." -ForegroundColor Green

# ---- .env check ----
Write-Step "Checking .env"
if (Test-Path ".env") {
    Write-Host ".env file found." -ForegroundColor Green
} else {
    Write-Host ".env file NOT found." -ForegroundColor Red
    Write-Host "You need to copy .env from your laptop. Create it in:" -ForegroundColor Yellow
    Write-Host "  $projectRoot\.env" -ForegroundColor Yellow
    Write-Host "The pipeline will work without it for now, but API ingestion won't." -ForegroundColor Yellow
}

# ---- Ollama ----
if (-not $SkipOllama) {
    Write-Step "Setting up Ollama"
    if (Test-Command "ollama") {
        Write-Host "Found: ollama" -ForegroundColor Green
    } else {
        Write-Host "Installing Ollama via winget..." -ForegroundColor Yellow
        winget install Ollama.Ollama --accept-package-agreements --accept-source-agreements
        $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
    }

    if (Test-Command "ollama") {
        Write-Host "Pulling model: $OllamaModel ..." -ForegroundColor Yellow
        ollama pull $OllamaModel
        Write-Host "Model $OllamaModel ready." -ForegroundColor Green

        Write-Host "`nVerifying Ollama models:" -ForegroundColor Yellow
        ollama list
    } else {
        Write-Host "Ollama installed but not in PATH. Restart terminal, then run:" -ForegroundColor Yellow
        Write-Host "  ollama pull $OllamaModel" -ForegroundColor Yellow
    }
} else {
    Write-Host "`nSkipping Ollama (--SkipOllama flag set)" -ForegroundColor Yellow
}

# ---- Claude Code ----
if (-not $SkipClaudeCode) {
    Write-Step "Setting up Claude Code"
    $npmGlobal = npm list -g @anthropic-ai/claude-code 2>&1
    if ($npmGlobal -match "claude-code") {
        Write-Host "Claude Code already installed." -ForegroundColor Green
    } else {
        Write-Host "Installing Claude Code..." -ForegroundColor Yellow
        npm install -g @anthropic-ai/claude-code
    }

    Write-Host "`nClaude Code installed. On first run it will ask for your API key." -ForegroundColor Green
    Write-Host "Test with: claude `"Say hello`"" -ForegroundColor Yellow
} else {
    Write-Host "`nSkipping Claude Code (--SkipClaudeCode flag set)" -ForegroundColor Yellow
}

# ---- Initialize database ----
Write-Step "Initializing database"
python -m insights_generator.cli init-db
Write-Host "Database initialized." -ForegroundColor Green

# ---- Summary ----
Write-Step "BOOTSTRAP COMPLETE"
Write-Host ""
Write-Host "Project root:  $projectRoot" -ForegroundColor White
Write-Host "Python venv:   $projectRoot\.venv" -ForegroundColor White
Write-Host "Ollama model:  $OllamaModel" -ForegroundColor White
Write-Host ""
Write-Host "NEXT STEPS:" -ForegroundColor Cyan
Write-Host "  1. Copy .env from your laptop (if not done yet)" -ForegroundColor White
Write-Host "  2. Authenticate Claude Code:  claude" -ForegroundColor White
Write-Host "  3. Test the pipeline:" -ForegroundColor White
Write-Host "       .venv\Scripts\Activate.ps1" -ForegroundColor Gray
Write-Host "       python -m insights_generator.cli scrape" -ForegroundColor Gray
Write-Host "       python -m insights_generator.cli analyze" -ForegroundColor Gray
Write-Host "       python -m insights_generator.cli status" -ForegroundColor Gray
Write-Host "  4. To launch workers, see agent_office/task_board.md" -ForegroundColor White
Write-Host ""

Pop-Location
