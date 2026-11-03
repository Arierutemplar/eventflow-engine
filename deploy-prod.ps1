$ErrorActionPreference = "Stop"

$composeFile = "docker-compose.prod.yml"
$services = @("api-gateway", "postgres", "rabbitmq", "prometheus", "grafana")
$timeoutSeconds = if ($env:DEPLOY_TIMEOUT_SECONDS) { [int]$env:DEPLOY_TIMEOUT_SECONDS } else { 180 }

function Invoke-Compose {
    param([string[]]$Arguments)
    & docker compose -f $composeFile @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }
}

try {
    Write-Host "==> Building and starting production stack"
    Invoke-Compose @("up", "-d", "--build")

    Write-Host "==> Waiting for core services (timeout: ${timeoutSeconds}s)"
    $deadline = (Get-Date).AddSeconds($timeoutSeconds)
    $ready = $false
    while ((Get-Date) -lt $deadline) {
        $ready = $true
        foreach ($service in $services) {
            $status = (& docker compose -f $composeFile ps --format "{{.State}} {{.Health}}" $service | Out-String)
            if ($status -notmatch "running") {
                $ready = $false
            }
            if (($service -eq "postgres" -or $service -eq "rabbitmq") -and $status -notmatch "healthy") {
                $ready = $false
            }
        }
        try { Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:3002/health" -TimeoutSec 3 | Out-Null } catch { $ready = $false }
        try { Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:9090/-/healthy" -TimeoutSec 3 | Out-Null } catch { $ready = $false }
        try { Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:3000/api/health" -TimeoutSec 3 | Out-Null } catch { $ready = $false }
        if ($ready) { break }
        Start-Sleep -Seconds 5
    }

    if (-not $ready) {
        throw "Timed out waiting for production services."
    }

    Write-Host "==> Production services"
    Invoke-Compose @("ps")
    Write-Host ""
    Write-Host "API Gateway: http://localhost:3002/health"
    Write-Host "Grafana:     http://localhost:3000"
    Write-Host "Prometheus:  http://localhost:9090"
}
catch {
    Write-Error $_
    & docker compose -f $composeFile ps
    exit 1
}
