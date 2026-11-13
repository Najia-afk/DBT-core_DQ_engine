$project = "mdm2-smoke-" + [Guid]::NewGuid().ToString("N").Substring(0, 8)
$compose = Join-Path $PSScriptRoot "compose.yml"
if (-not $env:MDM_DB_PASSWORD) {
    throw "Set MDM_DB_PASSWORD to a local URI-safe password before running this test."
}

function Invoke-Compose([string[]]$Options) {
    & docker compose -p $project -f $compose @Options
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Compose failed: $($Options -join ' ')"
    }
}

try {
    Invoke-Compose @("up", "-d", "--build")
    Invoke-Compose @("exec", "-T", "worker", "python", "-m", "engine.core.synth",
        "generate", "--nodes", "2000", "--attributes", "30", "--sources", "3",
        "--kinds", "parquet,parquet,parquet", "--workers", "1", "--days", "0",
        "--out", "/data/smoke")
    Invoke-Compose @("exec", "-T", "worker", "python", "-m", "engine.core.synth",
        "stage", "--out", "/data/smoke", "--day", "0")
    $submitted = Invoke-Compose @("exec", "-T", "worker", "python", "-m", "engine.runtime",
        "submit", "--config", "/data/smoke/config.yml", "--lane", "etl",
        "--key", "packaged-smoke", "--include-init", "--include-export",
        "--export-out", "/data/export")
    $id = ($submitted | ConvertFrom-Json).run_id
    $complete = $false
    for ($i = 0; $i -lt 40; $i++) {
        Start-Sleep -Seconds 2
        $status = Invoke-Compose @("exec", "-T", "worker", "python", "-m", "engine.runtime",
            "status", "--run-id", $id)
        $run = $status | ConvertFrom-Json
        if ($run.status -in @("succeeded", "succeeded_with_warnings")) {
            $run | Select-Object run_id, status | ConvertTo-Json
            $complete = $true
            break
        }
        if ($run.status -in @("failed", "needs_attention", "cancelled")) {
            throw $status
        }
    }
    if (-not $complete) {
        throw "The packaged run did not finish in time."
    }
    Invoke-Compose @("exec", "-T", "worker", "python", "-c",
        "from pathlib import Path; from engine.lanes.elt.render import TEMPLATES; assert TEMPLATES.exists(); files=list(Path('/data/export').glob('*.parquet')); assert len(files)==11, files; print('packaged templates and 11 canonical exports: OK')")
}
finally {
    Invoke-Compose @("logs", "--tail", "8", "worker")
    Invoke-Compose @("down", "--volumes")
}
