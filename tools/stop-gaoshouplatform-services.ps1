[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectRoot,
    [Parameter(Mandatory = $true)]
    [int]$BackendPort,
    [Parameter(Mandatory = $true)]
    [int]$SyncPort,
    [Parameter(Mandatory = $true)]
    [int]$FrontendPort,
    [ValidateRange(1, 120)]
    [int]$GracefulTimeoutSeconds = 20
)

$ErrorActionPreference = "Stop"
$ResolvedProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot)

function Test-ServiceCommandIdentity {
    param(
        [string]$CommandLine,
        [string]$Application,
        [string]$PidFile
    )

    if (-not $CommandLine) {
        return $false
    }
    $applicationPattern = [regex]::Escape($Application)
    $resolvedPidFile = [System.IO.Path]::GetFullPath($PidFile)
    $pidFilePattern = [regex]::Escape($resolvedPidFile)
    $runnerPattern = '(?:^|\s)-m\s+app\.service_runner\s+' + $applicationPattern + '(?=\s|$)'
    $pidPattern = '--pid-file(?:=|\s+)(?:"' + $pidFilePattern + '"|' + $pidFilePattern + ')(?=\s|$)'
    return [bool]($CommandLine -match $runnerPattern -and $CommandLine -match $pidPattern)
}

function Test-FrontendCommandIdentity {
    param(
        [string]$CommandLine,
        [string]$ProjectRoot
    )

    if (-not $CommandLine) {
        return $false
    }
    $frontendRoot = [System.IO.Path]::GetFullPath((Join-Path $ProjectRoot "frontend"))
    $normalizedCommand = $CommandLine.Replace('/', '\')
    $normalizedFrontendRoot = $frontendRoot.Replace('/', '\').TrimEnd('\') + '\'
    return (
        $normalizedCommand.IndexOf(
            $normalizedFrontendRoot,
            [System.StringComparison]::OrdinalIgnoreCase
        ) -ge 0 `
        -and $normalizedCommand -match 'node_modules\\vite|vite\.js'
    )
}

function Get-PersistedProcessId {
    param([string]$PidFile)

    if (-not (Test-Path -LiteralPath $PidFile -PathType Leaf)) {
        return $null
    }
    $pidContent = Get-Content -LiteralPath $PidFile -Raw -ErrorAction SilentlyContinue
    if ($null -eq $pidContent) {
        return $null
    }
    $rawProcessId = ([string]$pidContent).Trim()
    if ($rawProcessId -notmatch '^[1-9]\d*$') {
        return $null
    }
    try {
        return [int]$rawProcessId
    }
    catch {
        return $null
    }
}

function Get-ManagedServiceProcess {
    param(
        [int]$Port,
        [string]$Application,
        [string]$PidFile
    )

    $persistedProcessId = Get-PersistedProcessId -PidFile $PidFile
    if ($null -eq $persistedProcessId) {
        return $null
    }
    $ownsListener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
        Where-Object { [int]$_.OwningProcess -eq $persistedProcessId } |
        Select-Object -First 1
    if (-not $ownsListener) {
        return $null
    }
    $process = Get-CimInstance Win32_Process -Filter ("ProcessId=" + $persistedProcessId) -ErrorAction SilentlyContinue
    if (
        $process `
        -and (Test-ServiceCommandIdentity `
            -CommandLine $process.CommandLine `
            -Application $Application `
            -PidFile $PidFile)
    ) {
        return $process
    }
    return $null
}

function Request-GracefulShutdown {
    param(
        [int]$Port,
        [int]$ProcessId
    )

    try {
        $response = Invoke-WebRequest `
            -Uri ("http://127.0.0.1:$Port/internal/shutdown") `
            -Method Post `
            -Headers @{ "X-Gaoshou-Process-ID" = $ProcessId.ToString() } `
            -UseBasicParsing `
            -TimeoutSec 3
        return $response.StatusCode -eq 202
    }
    catch {
        return $false
    }
}

function Wait-ProcessExit {
    param(
        [int]$ProcessId,
        [int]$TimeoutSeconds
    )

    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    while ($timer.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        if (-not (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)) {
            return $true
        }
        Start-Sleep -Milliseconds 250
    }
    return -not [bool](Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)
}

function Stop-ManagedService {
    param(
        [string]$Label,
        [int]$Port,
        [string]$Application,
        [string]$PidFile
    )

    $process = Get-ManagedServiceProcess -Port $Port -Application $Application -PidFile $PidFile
    if (-not $process) {
        Write-Host ("      {0}: no verified project-owned process" -f $Label)
        return
    }

    $processId = [int]$process.ProcessId
    $requested = Request-GracefulShutdown -Port $Port -ProcessId $processId
    if ($requested) {
        Write-Host ("      {0}: graceful shutdown requested for PID {1}" -f $Label, $processId)
        if (Wait-ProcessExit -ProcessId $processId -TimeoutSeconds $GracefulTimeoutSeconds) {
            Write-Host ("      {0}: stopped gracefully" -f $Label)
            return
        }
        Write-Host ("      {0}: graceful shutdown timed out after {1}s" -f $Label, $GracefulTimeoutSeconds)
    }
    else {
        Write-Host ("      {0}: graceful endpoint unavailable" -f $Label)
    }

    $revalidated = Get-ManagedServiceProcess -Port $Port -Application $Application -PidFile $PidFile
    if (-not $revalidated -or [int]$revalidated.ProcessId -ne $processId) {
        Write-Host ("      {0}: identity changed; forced fallback skipped" -f $Label)
        return
    }
    Write-Host ("      {0}: forcing verified PID {1} as fallback" -f $Label, $processId)
    Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
}

function Get-ManagedFrontendProcesses {
    param(
        [int]$Port,
        [string]$ProjectRoot
    )

    $processes = @()
    $connections = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    foreach ($connection in $connections) {
        $process = Get-CimInstance Win32_Process -Filter ("ProcessId=" + $connection.OwningProcess) -ErrorAction SilentlyContinue
        if (
            $process `
            -and (Test-FrontendCommandIdentity `
                -CommandLine $process.CommandLine `
                -ProjectRoot $ProjectRoot)
        ) {
            $processes += $process
        }
    }
    return $processes
}

function Stop-FrontendProcesses {
    param(
        [int]$Port,
        [string]$ProjectRoot
    )

    $processes = @(Get-ManagedFrontendProcesses -Port $Port -ProjectRoot $ProjectRoot)
    Write-Host "      frontend processes may be stopped immediately."
    foreach ($process in $processes) {
        $processId = [int]$process.ProcessId
        $revalidated = @(
            Get-ManagedFrontendProcesses -Port $Port -ProjectRoot $ProjectRoot |
                Where-Object { [int]$_.ProcessId -eq $processId }
        )
        if ($revalidated.Count -eq 0) {
            Write-Host "      Frontend: identity changed; stop skipped"
            continue
        }
        Write-Host ("      Frontend: stopping verified PID {0}" -f $processId)
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
    }
}

function Invoke-GaoshouPlatformServiceStop {
    $runtimeRoot = Join-Path $ResolvedProjectRoot ".runtime"
    Stop-ManagedService `
        -Label "Backend API" `
        -Port $BackendPort `
        -Application "app.main:app" `
        -PidFile (Join-Path $runtimeRoot "backend-api.pid")
    Stop-ManagedService `
        -Label "Sync service" `
        -Port $SyncPort `
        -Application "app.sync_main:app" `
        -PidFile (Join-Path $runtimeRoot "sync-service.pid")
    Stop-FrontendProcesses -Port $FrontendPort -ProjectRoot $ResolvedProjectRoot
}

if ($MyInvocation.InvocationName -ne '.') {
    Invoke-GaoshouPlatformServiceStop
}
