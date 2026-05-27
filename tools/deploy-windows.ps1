param(
    [ValidateSet("development", "production")]
    [string]$Environment = $(if ($env:GAOSHOU_DEPLOY_ENV) { $env:GAOSHOU_DEPLOY_ENV } else { "development" }),
    [string]$Root = "",
    [string]$Branch = "",
    [string]$EnvFile = "",
    [string]$PipIndexUrl = $(if ($env:GAOSHOU_PIP_INDEX_URL) { $env:GAOSHOU_PIP_INDEX_URL } else { "https://pypi.org/simple" }),
    [switch]$AllowDirty,
    [switch]$SkipInstall
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-Defaults {
    param([string]$Name)

    if ($Name -eq "production") {
        return @{
            Root = "E:\Projects\GaoshouPlatform-prod"
            Branch = "main"
            BackendPort = "8800"
            SyncPort = "8810"
            FrontendPort = "3500"
        }
    }

    return @{
        Root = "E:\Projects\GaoshouPlatform-dev"
        Branch = "develop"
        BackendPort = "18800"
        SyncPort = "18810"
        FrontendPort = "13500"
    }
}

function Invoke-Checked {
    param(
        [string]$FilePath,
        [string[]]$ArgumentList,
        [string]$WorkingDirectory = (Get-Location).Path
    )

    Write-Host ">> $FilePath $($ArgumentList -join ' ')"
    $process = Start-Process -FilePath $FilePath -ArgumentList $ArgumentList -WorkingDirectory $WorkingDirectory -NoNewWindow -Wait -PassThru
    if ($process.ExitCode -ne 0) {
        throw "Command failed with exit code $($process.ExitCode): $FilePath $($ArgumentList -join ' ')"
    }
}

function Wait-HttpOk {
    param(
        [string]$Url,
        [int]$TimeoutSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 2 | Out-Null
            return
        } catch {
            Start-Sleep -Seconds 1
        }
    } while ((Get-Date) -lt $deadline)

    throw "Health check failed: $Url"
}

$defaults = Resolve-Defaults -Name $Environment
if (-not $Root) { $Root = $defaults.Root }
if (-not $Branch) { $Branch = $defaults.Branch }
if (-not $EnvFile) { $EnvFile = Join-Path $Root ".env.local" }

$backendPort = if ($env:GAOSHOU_BACKEND_PORT) { $env:GAOSHOU_BACKEND_PORT } else { $defaults.BackendPort }
$syncPort = if ($env:GAOSHOU_SYNC_PORT) { $env:GAOSHOU_SYNC_PORT } else { $defaults.SyncPort }
$frontendPort = if ($env:GAOSHOU_FRONTEND_PORT) { $env:GAOSHOU_FRONTEND_PORT } else { $defaults.FrontendPort }

$Root = (Resolve-Path -LiteralPath $Root).Path
$backendDir = Join-Path $Root "backend"
$frontendDir = Join-Path $Root "frontend"
$startScript = Join-Path $Root "tools\start-gaoshouplatform.bat"
$stopScript = Join-Path $Root "tools\stop-gaoshouplatform.bat"
$python = Join-Path $backendDir ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath (Join-Path $Root ".git"))) {
    throw "Deployment root is not a git checkout: $Root"
}
if (-not (Test-Path -LiteralPath $EnvFile)) {
    throw "Environment file is missing: $EnvFile"
}

Write-Host "========================================"
Write-Host "  GaoshouPlatform Windows Deploy"
Write-Host "========================================"
Write-Host "Environment: $Environment"
Write-Host "Root:        $Root"
Write-Host "Branch:      $Branch"
Write-Host "Env file:    $EnvFile"
Write-Host "Ports:       backend=$backendPort sync=$syncPort frontend=$frontendPort"
Write-Host "Pip index:   $PipIndexUrl"
Write-Host ""

$dirty = (& git -C $Root status --porcelain)
if ($dirty -and -not $AllowDirty) {
    throw "Deployment checkout has uncommitted changes. Commit/stash them or rerun with -AllowDirty."
}

Invoke-Checked -FilePath "git" -ArgumentList @("-C", $Root, "fetch", "origin", $Branch)
$currentBranch = (& git -C $Root branch --show-current).Trim()
if ($currentBranch -ne $Branch) {
    $localBranches = (& git -C $Root branch --format "%(refname:short)")
    if ($localBranches -contains $Branch) {
        Invoke-Checked -FilePath "git" -ArgumentList @("-C", $Root, "switch", $Branch)
    } else {
        Invoke-Checked -FilePath "git" -ArgumentList @("-C", $Root, "switch", "--track", "origin/$Branch")
    }
}
Invoke-Checked -FilePath "git" -ArgumentList @("-C", $Root, "pull", "--ff-only", "origin", $Branch)

if (-not (Test-Path -LiteralPath $python)) {
    $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($pyLauncher) {
        Invoke-Checked -FilePath "py" -ArgumentList @("-3.12", "-m", "venv", ".venv") -WorkingDirectory $backendDir
    } else {
        Invoke-Checked -FilePath "python" -ArgumentList @("-m", "venv", ".venv") -WorkingDirectory $backendDir
    }
}

if (-not $SkipInstall) {
    Invoke-Checked -FilePath $python -ArgumentList @("-m", "pip", "install", "--index-url", $PipIndexUrl, "--timeout", "60", "--retries", "5", "--upgrade", "pip") -WorkingDirectory $backendDir
    Invoke-Checked -FilePath $python -ArgumentList @("-m", "pip", "install", "--index-url", $PipIndexUrl, "--timeout", "60", "--retries", "5", "-e", ".[dev]") -WorkingDirectory $backendDir
    Invoke-Checked -FilePath "npm" -ArgumentList @("ci") -WorkingDirectory $frontendDir
}

Invoke-Checked -FilePath "npm" -ArgumentList @("run", "build") -WorkingDirectory $frontendDir

$env:GAOSHOU_ROOT = $Root
$env:GAOSHOU_ENV_FILE = $EnvFile
$env:GAOSHOU_BACKEND_PORT = $backendPort
$env:GAOSHOU_SYNC_PORT = $syncPort
$env:GAOSHOU_FRONTEND_PORT = $frontendPort
$env:GAOSHOU_SKIP_PAUSE = "1"

Invoke-Checked -FilePath "cmd.exe" -ArgumentList @("/c", "`"$stopScript`" --no-pause") -WorkingDirectory $Root
Invoke-Checked -FilePath "cmd.exe" -ArgumentList @("/c", "`"$startScript`" --no-pause") -WorkingDirectory $Root

Wait-HttpOk -Url "http://127.0.0.1:$backendPort/health" -TimeoutSeconds 60
Wait-HttpOk -Url "http://127.0.0.1:$syncPort/health" -TimeoutSeconds 60
Wait-HttpOk -Url "http://127.0.0.1:$frontendPort" -TimeoutSeconds 60

Write-Host ""
Write-Host "Deployment completed: $Environment"
