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

function Invoke-GitChecked {
    param(
        [string]$DeploymentRoot,
        [string[]]$ArgumentList
    )

    Invoke-Checked -FilePath "git" `
        -ArgumentList (@("-c", "safe.directory=$DeploymentRoot", "-C", $DeploymentRoot) + $ArgumentList)
}

function Invoke-GitOutput {
    param(
        [string]$DeploymentRoot,
        [string[]]$ArgumentList
    )

    return (& git -c "safe.directory=$DeploymentRoot" -C $DeploymentRoot @ArgumentList)
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

function Get-EnvFileMap {
    param([string]$Path)

    $result = @{}
    if (-not (Test-Path -LiteralPath $Path)) {
        return $result
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#")) {
            continue
        }
        $separator = $trimmed.IndexOf("=")
        if ($separator -lt 1) {
            continue
        }
        $key = $trimmed.Substring(0, $separator).Trim()
        $value = $trimmed.Substring($separator + 1).Trim()
        if ($key) {
            $result[$key] = $value
        }
    }

    return $result
}

function Test-PythonImport {
    param(
        [string]$PythonPath,
        [string]$ModuleName
    )

    $escaped = $ModuleName.Replace("'", "''")
    & $PythonPath -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('$escaped') else 1)" *> $null
    return $LASTEXITCODE -eq 0
}

function Invoke-CmdChecked {
    param(
        [string]$Command,
        [string]$WorkingDirectory = (Get-Location).Path
    )

    $cmdExe = if ($env:ComSpec) { $env:ComSpec } else { "cmd.exe" }
    Invoke-Checked -FilePath $cmdExe -ArgumentList @("/d", "/s", "/c", "`"$Command`"") -WorkingDirectory $WorkingDirectory
}

function Invoke-NpmChecked {
    param(
        [string[]]$ArgumentList,
        [string]$WorkingDirectory = (Get-Location).Path
    )

    $npmCmd = (Get-Command npm.cmd -ErrorAction Stop).Source
    Invoke-Checked -FilePath $npmCmd -ArgumentList $ArgumentList -WorkingDirectory $WorkingDirectory
}

function Set-EnvVarOrRemove {
    param(
        [string]$Name,
        [bool]$HadValue,
        [AllowNull()][string]$Value
    )

    if ($HadValue) {
        Set-Item -LiteralPath "Env:$Name" -Value $Value
    } else {
        Remove-Item -LiteralPath "Env:$Name" -ErrorAction SilentlyContinue
    }
}

function Invoke-StartScriptChecked {
    param(
        [string]$StartScript,
        [string]$WorkingDirectory
    )

    $hadRunnerTracking = Test-Path -LiteralPath "Env:RUNNER_TRACKING_ID"
    $oldRunnerTracking = if ($hadRunnerTracking) { $env:RUNNER_TRACKING_ID } else { $null }
    $hadSkipOptional = Test-Path -LiteralPath "Env:GAOSHOU_SKIP_OPTIONAL_CHECKS"
    $oldSkipOptional = if ($hadSkipOptional) { $env:GAOSHOU_SKIP_OPTIONAL_CHECKS } else { $null }
    $hadSkipDocker = Test-Path -LiteralPath "Env:GAOSHOU_SKIP_DOCKER"
    $oldSkipDocker = if ($hadSkipDocker) { $env:GAOSHOU_SKIP_DOCKER } else { $null }

    try {
        # Why: GitHub's Windows runner kills tracked child processes after the
        # job. Services launched by the startup script must survive deployment.
        Remove-Item -LiteralPath "Env:RUNNER_TRACKING_ID" -ErrorAction SilentlyContinue
        $env:GAOSHOU_SKIP_OPTIONAL_CHECKS = "1"
        $env:GAOSHOU_SKIP_DOCKER = "1"
        Invoke-CmdChecked -Command "`"$StartScript`" --no-pause" -WorkingDirectory $WorkingDirectory
    } finally {
        Set-EnvVarOrRemove -Name "RUNNER_TRACKING_ID" -HadValue $hadRunnerTracking -Value $oldRunnerTracking
        Set-EnvVarOrRemove -Name "GAOSHOU_SKIP_OPTIONAL_CHECKS" -HadValue $hadSkipOptional -Value $oldSkipOptional
        Set-EnvVarOrRemove -Name "GAOSHOU_SKIP_DOCKER" -HadValue $hadSkipDocker -Value $oldSkipDocker
    }
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
$envConfig = Get-EnvFileMap -Path $EnvFile

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

$dirty = Invoke-GitOutput -DeploymentRoot $Root -ArgumentList @("status", "--porcelain")
if ($dirty -and -not $AllowDirty) {
    throw "Deployment checkout has uncommitted changes. Commit/stash them or rerun with -AllowDirty."
}

Invoke-GitChecked -DeploymentRoot $Root -ArgumentList @("fetch", "origin", $Branch)
$currentBranch = (Invoke-GitOutput -DeploymentRoot $Root -ArgumentList @("branch", "--show-current")).Trim()
if ($currentBranch -ne $Branch) {
    $localBranches = Invoke-GitOutput -DeploymentRoot $Root -ArgumentList @("branch", "--format", "%(refname:short)")
    if ($localBranches -contains $Branch) {
        Invoke-GitChecked -DeploymentRoot $Root -ArgumentList @("switch", $Branch)
    } else {
        Invoke-GitChecked -DeploymentRoot $Root -ArgumentList @("switch", "--track", "origin/$Branch")
    }
}
Invoke-GitChecked -DeploymentRoot $Root -ArgumentList @("pull", "--ff-only", "origin", $Branch)

if (-not (Test-Path -LiteralPath $python)) {
    $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($pyLauncher) {
        Invoke-Checked -FilePath "py" -ArgumentList @("-3.12", "-m", "venv", ".venv") -WorkingDirectory $backendDir
    } else {
        Invoke-Checked -FilePath "python" -ArgumentList @("-m", "venv", ".venv") -WorkingDirectory $backendDir
    }
}

$env:GAOSHOU_ROOT = $Root
$env:GAOSHOU_ENV_FILE = $EnvFile
$env:GAOSHOU_BACKEND_PORT = $backendPort
$env:GAOSHOU_SYNC_PORT = $syncPort
$env:GAOSHOU_FRONTEND_PORT = $frontendPort
$env:GAOSHOU_SKIP_PAUSE = "1"
$env:GAOSHOU_SKIP_OPTIONAL_CHECKS = "1"
$env:GAOSHOU_SKIP_DOCKER = "1"

$servicesStopped = $false
$servicesStarted = $false

try {
    # Why: npm ci replaces native bindings under node_modules and will fail on
    # Windows when the running Vite dev server keeps those files open.
    Invoke-CmdChecked -Command "`"$stopScript`" --no-pause" -WorkingDirectory $Root
    $servicesStopped = $true

    if (-not $SkipInstall) {
        Invoke-Checked -FilePath $python -ArgumentList @("-m", "pip", "install", "--index-url", $PipIndexUrl, "--timeout", "60", "--retries", "5", "--upgrade", "pip") -WorkingDirectory $backendDir
        Invoke-Checked -FilePath $python -ArgumentList @("-m", "pip", "install", "--index-url", $PipIndexUrl, "--timeout", "60", "--retries", "5", "packaging", "hatchling", "editables") -WorkingDirectory $backendDir
        Invoke-Checked -FilePath $python -ArgumentList @("-m", "pip", "install", "--index-url", $PipIndexUrl, "--timeout", "60", "--retries", "5", "--no-build-isolation", "-e", ".[dev]") -WorkingDirectory $backendDir

        $xtquantSpec = if ($env:GAOSHOU_XTQUANT_SPEC) {
            $env:GAOSHOU_XTQUANT_SPEC
        } elseif ($envConfig.ContainsKey("GAOSHOU_XTQUANT_SPEC") -and $envConfig["GAOSHOU_XTQUANT_SPEC"]) {
            $envConfig["GAOSHOU_XTQUANT_SPEC"]
        } else {
            "xtquant==250516.1.1"
        }
        $needsQmt = $envConfig.ContainsKey("QMT_ACCOUNT_ID") -or $envConfig.ContainsKey("QMT_TRADER_PATH")
        if ($needsQmt -and -not (Test-PythonImport -PythonPath $python -ModuleName "xtquant")) {
            Invoke-Checked -FilePath $python -ArgumentList @("-m", "pip", "install", "--index-url", $PipIndexUrl, "--timeout", "60", "--retries", "5", $xtquantSpec) -WorkingDirectory $backendDir
        }

        Invoke-NpmChecked -ArgumentList @("ci") -WorkingDirectory $frontendDir
    }

    Invoke-NpmChecked -ArgumentList @("run", "build") -WorkingDirectory $frontendDir
    Invoke-StartScriptChecked -StartScript $startScript -WorkingDirectory $Root
    $servicesStarted = $true

    Wait-HttpOk -Url "http://127.0.0.1:$backendPort/health" -TimeoutSeconds 60
    Wait-HttpOk -Url "http://127.0.0.1:$syncPort/health" -TimeoutSeconds 60
    Wait-HttpOk -Url "http://127.0.0.1:$frontendPort" -TimeoutSeconds 60
} catch {
    if ($servicesStopped -and -not $servicesStarted) {
        Write-Warning "Deployment failed before restart. Attempting to bring the target environment back online."
        try {
            Invoke-StartScriptChecked -StartScript $startScript -WorkingDirectory $Root
            $servicesStarted = $true
        } catch {
            Write-Warning "Automatic restart after deploy failure also failed: $($_.Exception.Message)"
        }
    }
    throw
}

Write-Host ""
Write-Host "Deployment completed: $Environment"
